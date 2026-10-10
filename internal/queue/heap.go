package queue

import "container/heap"

// Two heaps hold jobs that are waiting:
//   ready   - runnable now, ordered by (priority desc, submit order asc)
//   delayed - waiting out a retry backoff, ordered by NextRun asc
// Each Job remembers its heap position so apply() can remove it from the middle (replay may lease any job).

type readyHeap []*Job

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].seq < h[j].seq
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].heapIdx = i; h[j].heapIdx = j }
func (h *readyHeap) Push(x any)   { j := x.(*Job); j.heapIdx = len(*h); *h = append(*h, j) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	j.heapIdx = -1
	return j
}

type delayedHeap []*Job

func (h delayedHeap) Len() int           { return len(h) }
func (h delayedHeap) Less(i, j int) bool { return h[i].NextRun < h[j].NextRun }
func (h delayedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].heapIdx = i; h[j].heapIdx = j }
func (h *delayedHeap) Push(x any)        { j := x.(*Job); j.heapIdx = len(*h); *h = append(*h, j) }
func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	j.heapIdx = -1
	return j
}

var (
	_ heap.Interface = (*readyHeap)(nil)
	_ heap.Interface = (*delayedHeap)(nil)
)
