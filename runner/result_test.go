package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponseValidation(t *testing.T) {
	failure := errors.New("failure")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name    string
		results []Result
		count   int
		aborted bool
		valid   bool
	}{
		{"nil", nil, 3, false, true},
		{"empty", []Result{}, 0, false, true},
		{"empty nonnil", []Result{}, 1, false, false},
		{"short", []Result{{}}, 2, false, false},
		{"long", []Result{{}, {}}, 1, false, false},
		{"success", []Result{{}}, 1, false, true},
		{"success error", []Result{{Err: failure}}, 1, false, false},
		{"failed", []Result{{State: ResultFailed, Err: failure}}, 1, false, true},
		{"failed nil", []Result{{State: ResultFailed}}, 1, true, false},
		{"unattempted", []Result{{State: ResultUnattempted}}, 1, true, true},
		{"unattempted live", []Result{{State: ResultUnattempted}}, 1, false, false},
		{
			"unattempted error", []Result{{State: ResultUnattempted, Err: failure}},
			1, true, false,
		},
		{"interrupted", []Result{PeerCanceled(ctx)}, 1, true, true},
		{"unknown", []Result{{State: ResultState(255)}}, 1, true, false},
		{
			"mixed", []Result{{}, {State: ResultFailed, Err: failure}, PeerCanceled(ctx)},
			3, true, true,
		},
		{"late invalid", []Result{{}, {State: ResultState(255)}}, 2, true, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := validateResults(test.results, test.count, test.aborted)
			if test.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
	results := []Result{{}, {Err: failure}}
	err := validateResults(results, 2, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "result 1")
	assert.Equal(t, []Result{{}, {Err: failure}}, results)
}
