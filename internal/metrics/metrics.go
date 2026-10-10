// Package metrics writes the Prometheus text exposition format by hand (no dependency): counters, gauges and a
// fixed-bucket histogram. Only what jobq needs.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing value, optionally with labels (one Counter per label set).
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// Histogram has cumulative buckets as Prometheus expects. Observe is lock-free.
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64 // counts[i] = observations <= bounds[i]; the last slot is +Inf
	sumNS  atomic.Uint64   // sum in micro-units to stay integral (value*1e6)
	count  atomic.Uint64
}

// NewHistogram creates a histogram with the given upper bounds (seconds), sorted ascending.
func NewHistogram(bounds ...float64) *Histogram {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	return &Histogram{bounds: b, counts: make([]atomic.Uint64, len(b)+1)}
}

func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // first bound >= v
	h.counts[i].Add(1)
	h.count.Add(1)
	h.sumNS.Add(uint64(math.Max(v, 0) * 1e6))
}

// Registry collects named series and renders them.
type Registry struct {
	mu     sync.Mutex
	series []func(w io.Writer)
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f func(w io.Writer)) {
	r.mu.Lock()
	r.series = append(r.series, f)
	r.mu.Unlock()
}

func header(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func labelStr(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%q", k, labels[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Counter registers and returns a counter with fixed labels.
func (r *Registry) Counter(name, help string, labels map[string]string) *Counter {
	c := &Counter{}
	r.add(func(w io.Writer) {
		header(w, name, help, "counter")
		fmt.Fprintf(w, "%s%s %d\n", name, labelStr(labels), c.Value())
	})
	return c
}

// CounterFunc registers a counter read from a function at scrape time (for counters kept elsewhere).
func (r *Registry) CounterFunc(name, help string, f func() uint64) {
	r.add(func(w io.Writer) { header(w, name, help, "counter"); fmt.Fprintf(w, "%s %d\n", name, f()) })
}

// GaugeVec registers a gauge family whose label values and numbers are produced at scrape time.
func (r *Registry) GaugeVec(name, help, label string, f func() map[string]float64) {
	r.add(func(w io.Writer) {
		header(w, name, help, "gauge")
		vals := f()
		keys := make([]string, 0, len(vals))
		for k := range vals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%s{%s=%q} %g\n", name, label, k, vals[k])
		}
	})
}

// Gauge registers a single gauge read at scrape time.
func (r *Registry) Gauge(name, help string, f func() float64) {
	r.add(func(w io.Writer) { header(w, name, help, "gauge"); fmt.Fprintf(w, "%s %g\n", name, f()) })
}

// Histogram registers h under name.
func (r *Registry) Histogram(name, help string, h *Histogram) {
	r.add(func(w io.Writer) {
		header(w, name, help, "histogram")
		var cum uint64
		for i, b := range h.bounds {
			cum += h.counts[i].Load()
			fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b, cum)
		}
		cum += h.counts[len(h.bounds)].Load()
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cum)
		fmt.Fprintf(w, "%s_sum %g\n%s_count %d\n", name, float64(h.sumNS.Load())/1e6, name, h.count.Load())
	})
}

// Render writes every registered series.
func (r *Registry) Render(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range r.series {
		f(w)
	}
}
