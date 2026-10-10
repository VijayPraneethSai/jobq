package metrics

import (
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("jobs_total", "Jobs.", map[string]string{"outcome": "ok"})
	c.Inc()
	c.Add(2)
	r.Gauge("depth", "Depth.", func() float64 { return 7 })
	r.GaugeVec("jobs", "By state.", "state", func() map[string]float64 { return map[string]float64{"queued": 3, "running": 1} })
	h := NewHistogram(0.1, 1)
	for _, v := range []float64{0.05, 0.5, 0.5, 3} {
		h.Observe(v)
	}
	r.Histogram("latency_seconds", "Latency.", h)
	var sb strings.Builder
	r.Render(&sb)
	out := sb.String()
	for _, want := range []string{
		`jobs_total{outcome="ok"} 3`,
		"depth 7",
		`jobs{state="queued"} 3`,
		`latency_seconds_bucket{le="0.1"} 1`,
		`latency_seconds_bucket{le="1"} 3`,
		`latency_seconds_bucket{le="+Inf"} 4`,
		"latency_seconds_count 4",
		"latency_seconds_sum 4.05",
		"# TYPE latency_seconds histogram",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}
