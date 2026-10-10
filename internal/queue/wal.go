package queue

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SyncPolicy decides when WAL records are forced to disk (fsync).
type SyncPolicy int

const (
	// SyncBatch (default): records reach the OS on every write; a background goroutine fsyncs every 50 ms. A power
	// failure can lose up to ~50 ms of acknowledged changes; a process crash loses nothing (data is in the OS cache).
	SyncBatch SyncPolicy = iota
	// SyncAlways: fsync before every acknowledgement. Safest, slowest (limited by the disk's fsync latency).
	SyncAlways
	// SyncNever: never fsync (tests, benchmarks that measure the queue and not the disk).
	SyncNever
)

const (
	walName      = "wal.jsonl"
	snapshotName = "snapshot.jsonl"
)

// wal is an append-only file of JSON lines, one Event per line.
type wal struct {
	dir    string
	policy SyncPolicy

	mu    sync.Mutex
	f     *os.File
	w     *bufio.Writer
	dirty bool
	bytes int64

	stop chan struct{}
	done chan struct{}
}

func openWAL(dir string, policy SyncPolicy) (*wal, error) {
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &wal{dir: dir, policy: policy, f: f, w: bufio.NewWriterSize(f, 64<<10), bytes: st.Size(), stop: make(chan struct{}), done: make(chan struct{})}
	if policy == SyncBatch {
		go w.syncLoop()
	} else {
		close(w.done)
	}
	return w, nil
}

func (w *wal) syncLoop() {
	defer close(w.done)
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.mu.Lock()
			if w.dirty {
				_ = w.f.Sync()
				w.dirty = false
			}
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}
}

// append writes one event line. The caller (Store) holds the store lock, so records are written in apply order.
func (w *wal) append(e *Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	w.bytes += int64(len(b))
	if err := w.w.Flush(); err != nil { // hand the line to the OS now: survives a process crash
		return err
	}
	switch w.policy {
	case SyncAlways:
		return w.f.Sync()
	case SyncBatch:
		w.dirty = true
	}
	return nil
}

func (w *wal) size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes
}

// reset empties the log (after a snapshot that covers everything in it).
func (w *wal) reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	// O_APPEND: the next write goes to the new end of file (0), so no Seek is needed.
	w.bytes = 0
	w.dirty = false
	return w.f.Sync()
}

func (w *wal) close() error {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.policy != SyncNever {
		_ = w.f.Sync()
	}
	return w.f.Close()
}

// ---- replay ----

// replayResult describes what recovery found, for logging and tests.
type replayResult struct {
	SnapshotJobs   int
	Events         int
	Skipped        int   // events already covered by the snapshot
	TruncatedBytes int64 // bytes of a damaged/partial tail that were cut off
}

type snapshotHeader struct {
	LastSeq uint64 `json:"last_seq"`
	JobSeq  uint64 `json:"job_seq"`
}

// readSnapshot loads snapshot.jsonl if present. First line is a header, every further line is a Job.
func readSnapshot(dir string) (hdr snapshotHeader, jobs []*Job, err error) {
	f, err := os.Open(filepath.Join(dir, snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return hdr, nil, nil
	}
	if err != nil {
		return hdr, nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	first := true
	for {
		line, rerr := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			if first {
				if err := json.Unmarshal(line, &hdr); err != nil {
					return hdr, nil, fmt.Errorf("snapshot header: %w", err)
				}
				first = false
			} else {
				j := new(Job)
				if err := json.Unmarshal(line, j); err != nil {
					return hdr, nil, fmt.Errorf("snapshot job: %w", err)
				}
				jobs = append(jobs, j)
			}
		}
		if rerr == io.EOF {
			return hdr, jobs, nil
		}
		if rerr != nil {
			return hdr, nil, rerr
		}
	}
}

// readEvents streams the WAL and calls fn for each decodable event. A line that does not decode, or a last line
// without its newline (the process died mid-write), ends the replay and the file is truncated to the last good
// byte, so the next append continues from a clean log. Corruption in the MIDDLE of the file is treated the same way
// (everything after it is dropped) - the log has no checksums, which is listed as a limitation in the README.
func readEvents(dir string, fn func(*Event)) (events int, truncated int64, err error) {
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var good int64
	for {
		line, rerr := r.ReadBytes('\n')
		if rerr == io.EOF && len(line) == 0 {
			break
		}
		complete := rerr == nil
		var e Event
		if !complete || json.Unmarshal(line, &e) != nil {
			break // partial or corrupt record: stop here
		}
		fn(&e)
		events++
		good += int64(len(line))
	}
	st, err := f.Stat()
	if err != nil {
		return events, 0, err
	}
	if st.Size() > good {
		truncated = st.Size() - good
		if err := f.Truncate(good); err != nil {
			return events, truncated, err
		}
		_ = f.Sync()
	}
	return events, truncated, nil
}

// writeSnapshot writes the snapshot atomically: temp file, fsync, rename.
func writeSnapshot(dir string, hdr snapshotHeader, jobs []*Job) error {
	tmp := filepath.Join(dir, snapshotName+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)
	if err := enc.Encode(hdr); err != nil {
		f.Close()
		return err
	}
	for _, j := range jobs {
		if err := enc.Encode(j); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, snapshotName)); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil { // make the rename itself durable (best effort)
		_ = d.Sync()
		d.Close()
	}
	return nil
}
