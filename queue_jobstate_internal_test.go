// Ported from bullmq_rust/tests/queue_test.rs (test_job_state_parsing). The
// Go state parser is unexported, so this test lives in package bullmq.
package bullmq

import "testing"

func TestJobStateParsing(t *testing.T) {
	cases := []struct {
		in   string
		want JobState
	}{
		{"wait", StateWaiting},
		{"waiting", StateWaiting},
		{"active", StateActive},
		{"delayed", StateDelayed},
		{"completed", StateCompleted},
		{"failed", StateFailed},
		{"waiting-children", StateWaitingChildren},
		{"unknown_state", StateUnknown},
	}
	for _, c := range cases {
		if got := parseJobState(c.in); got != c.want {
			t.Errorf("parseJobState(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
