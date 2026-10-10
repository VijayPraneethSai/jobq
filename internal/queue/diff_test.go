package queue

import (
	"fmt"
	"reflect"
	"testing"
)

// diffJobs reports the first differing job and field names (test helper).
func diffJobs(a, b []Job) string {
	if len(a) != len(b) {
		return fmt.Sprintf("job count %d vs %d", len(a), len(b))
	}
	for i := range a {
		va, vb := reflect.ValueOf(a[i]), reflect.ValueOf(b[i])
		for f := 0; f < va.NumField(); f++ {
			if !va.Type().Field(f).IsExported() {
				continue
			}
			if !reflect.DeepEqual(va.Field(f).Interface(), vb.Field(f).Interface()) {
				return fmt.Sprintf("job %s field %s: %v vs %v", a[i].ID, va.Type().Field(f).Name, va.Field(f).Interface(), vb.Field(f).Interface())
			}
		}
	}
	return ""
}

func TestDiffHelperWorks(t *testing.T) {
	if diffJobs([]Job{{ID: "a"}}, []Job{{ID: "a", ExitCode: 1}}) == "" {
		t.Fatal("diff helper missed a difference")
	}
}
