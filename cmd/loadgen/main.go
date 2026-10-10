// Command loadgen measures the queue end to end: it starts a coordinator and W workers in this process (talking over
// real HTTP on loopback), submits N jobs and reports throughput and submit-to-finish latency percentiles.
//
// Everything shares this machine's cores, so the numbers describe "this machine, one process" and are not a statement
// about a deployment with separate hosts. Jobs run a real child process (default: /bin/true), so process start cost
// is included.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/client"
	"github.com/VijayPraneethSai/jobq/internal/queue"
	"github.com/VijayPraneethSai/jobq/internal/server"
	"github.com/VijayPraneethSai/jobq/internal/worker"
)

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return sorted[i]
}

func main() {
	n := flag.Int("n", 2000, "number of jobs")
	workers := flag.Int("workers", 8, "concurrent job slots (one worker process-equivalent with this concurrency)")
	submitters := flag.Int("submitters", 4, "concurrent submitting clients")
	rate := flag.Float64("rate", 0, "submit rate in jobs/s across all submitters (0 = as fast as possible)")
	syncPol := flag.String("sync", "batch", "WAL fsync policy: batch|always|never")
	cmd := flag.String("cmd", "/bin/true", "command each job runs")
	dir := flag.String("data", "", "WAL directory (default: a temp dir)")
	csv := flag.Bool("csv", false, "print one CSV line (for scripts)")
	flag.Parse()

	if *dir == "" {
		d, err := os.MkdirTemp("", "jobq-load-")
		if err != nil {
			panic(err)
		}
		defer func() { _ = os.RemoveAll(d) }()
		*dir = d
	}
	pol := queue.SyncBatch
	switch *syncPol {
	case "always":
		pol = queue.SyncAlways
	case "never":
		pol = queue.SyncNever
	}
	store, err := queue.Open(queue.Config{Dir: *dir, Sync: pol})
	if err != nil {
		panic(err)
	}
	defer func() { _ = store.Close() }()
	srv := server.New(store, server.Config{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	hs := &http.Server{Handler: srv.Handler()}
	go func() { _ = hs.Serve(ln) }()
	defer func() { _ = hs.Close() }()
	base := "http://" + ln.Addr().String()
	c := client.New(base, "")

	wctx, stopWorkers := context.WithCancel(context.Background())
	var wwg sync.WaitGroup
	wwg.Add(1)
	go func() {
		defer wwg.Done()
		worker.Run(wctx, worker.Config{Client: client.New(base, ""), ID: "load", Concurrency: *workers, PollInterval: 5 * time.Millisecond, Grace: time.Second})
	}()

	ctx := context.Background()
	start := time.Now()
	var next int
	var mu sync.Mutex
	var swg sync.WaitGroup
	for s := 0; s < *submitters; s++ {
		swg.Add(1)
		go func() {
			defer swg.Done()
			for {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				if i >= *n {
					return
				}
				if *rate > 0 { // open loop: job i is due at start + i/rate
					if d := time.Until(start.Add(time.Duration(float64(i) / *rate * float64(time.Second)))); d > 0 {
						time.Sleep(d)
					}
				}
				if _, _, err := c.Submit(ctx, client.SubmitReq{Command: *cmd}); err != nil {
					fmt.Fprintln(os.Stderr, "submit:", err)
					os.Exit(1)
				}
			}
		}()
	}
	swg.Wait()
	submitDone := time.Since(start)
	for { // wait for completion
		st := store.Stats()
		if st.Succeeded+st.Dead >= *n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	total := time.Since(start)
	stopWorkers()
	wwg.Wait()

	var lat []float64
	failed := 0
	for _, j := range store.List("", 0) {
		if j.State != queue.Succeeded {
			failed++
			continue
		}
		lat = append(lat, float64(j.FinishedAt-j.SubmittedAt)/1e6) // ms
	}
	sort.Float64s(lat)
	thr := float64(*n) / total.Seconds()
	if *csv {
		fmt.Printf("%d,%d,%d,%s,%.0f,%.0f,%.1f,%.1f,%.1f,%d\n", *n, *workers, *submitters, *syncPol, *rate, thr, pct(lat, .5), pct(lat, .95), pct(lat, .99), failed)
		return
	}
	fmt.Printf("machine: %d CPUs (GOMAXPROCS=%d), %s/%s, coordinator + workers + loadgen in one process, WAL sync=%s\n", runtime.NumCPU(), runtime.GOMAXPROCS(0), runtime.GOOS, runtime.GOARCH, *syncPol)
	fmt.Printf("jobs=%d workers=%d submitters=%d rate=%.0f/s cmd=%s\n", *n, *workers, *submitters, *rate, *cmd)
	fmt.Printf("submit phase %.2fs (%.0f jobs/s), all finished after %.2fs -> %.0f jobs/s end to end, failed=%d\n", submitDone.Seconds(), float64(*n)/submitDone.Seconds(), total.Seconds(), thr, failed)
	fmt.Printf("submit->finish latency ms: p50=%.1f p95=%.1f p99=%.1f max=%.1f\n", pct(lat, .5), pct(lat, .95), pct(lat, .99), pct(lat, 1))
}
