package runner

import "fmt"

// ResultState describes one position in a handler response. Unknown values are invalid.
type ResultState uint8

const (
	// ResultSucceeded establishes success and requires a nil error.
	ResultSucceeded ResultState = iota
	// ResultFailed reports an attempted operation and requires a non-nil error.
	ResultFailed
	// ResultUnattempted reports no attempt and requires a nil error and runner abort.
	ResultUnattempted
	// ResultInterrupted reports an attempted operation interrupted by peer cancellation.
	// Construct it with PeerCanceled after observing the supplied handler context cancel.
	ResultInterrupted
)

// Result describes a handler position. A nil response slice means every position succeeded.
type Result struct {
	// State selects the validation and classification rules for this position.
	State ResultState
	// Err must be non-nil only for ResultFailed or a valid PeerCanceled result.
	Err error
	// Capture validity at helper invocation, without retaining the handler context.
	peerCanceled bool
}

// TerminalState describes final ownership disposition, not exactly-once external effects.
type TerminalState uint8

const (
	// TerminalSucceeded means the handler established success, even if abort raced it.
	TerminalSucceeded TerminalState = iota
	// TerminalFailed means success was not established. Effects might already have occurred.
	TerminalFailed
	// TerminalAbandoned means work stopped before success, possibly after earlier attempts.
	TerminalAbandoned
)

// ItemOutcome is delivered once per accepted occurrence, including duplicate payload values.
type ItemOutcome struct {
	// State is the final disposition of this occurrence.
	State TerminalState
	// Attempts counts invocations containing this item, excluding unattempted positions.
	Attempts int
	// Err is nil on success, the terminal failure error on failure, and the latest ordinary error
	// on abandonment, or the abort cause when there was no ordinary error.
	Err error
}

func validateResults(results []Result, count int, aborted bool) error {
	if results == nil {
		return nil
	}
	if len(results) != count {
		return fmt.Errorf("runner: handler returned %d results for %d items", len(results), count)
	}
	for index, result := range results {
		if err := validateResult(result, aborted); err != nil {
			return fmt.Errorf("runner: result %d: %w", index, err)
		}
	}
	return nil
}

func validateResult(result Result, aborted bool) error {
	switch result.State {
	case ResultSucceeded:
		if result.Err != nil {
			return fmt.Errorf("succeeded result has an error: %w", result.Err)
		}
	case ResultFailed:
		if result.Err == nil {
			return fmt.Errorf("failed result has no error")
		}
	case ResultUnattempted:
		if result.Err != nil {
			return fmt.Errorf("unattempted result has an error: %w", result.Err)
		}
		if !aborted {
			return fmt.Errorf("unattempted result without abort")
		}
	case ResultInterrupted:
		if !result.peerCanceled || result.Err == nil {
			return fmt.Errorf("interrupted result without observed peer cancellation")
		}
	default:
		return fmt.Errorf("unknown result state: %d", result.State)
	}
	return nil
}
