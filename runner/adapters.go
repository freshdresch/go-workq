package runner

import "context"

// Items adapts a per-item handler. Ordinary errors, including context errors, become failures.
// It continues after failures and checks cancellation between items. Uncalled positions are
// unattempted. Failure policy therefore takes effect after the entire batch response returns.
func Items[T any](handler func(context.Context, T) error) func(context.Context, []T) []Result {
	if handler == nil {
		return nil
	}
	return func(ctx context.Context, items []T) []Result {
		results := make([]Result, len(items))
		for index, item := range items {
			if ctx.Err() != nil {
				for remaining := index; remaining < len(items); remaining++ {
					results[remaining].State = ResultUnattempted
				}
				break
			}
			if err := handler(ctx, item); err != nil {
				results[index] = Result{State: ResultFailed, Err: err}
			}
		}
		return results
	}
}

// WholeBatch adapts a batch operation. Nil means every position succeeded. An error fails every
// position with uncertain effects. Retrying requires every operation to tolerate duplicate effects.
func WholeBatch[T any](
	handler func(context.Context, []T) error,
) func(context.Context, []T) []Result {
	if handler == nil {
		return nil
	}
	return func(ctx context.Context, items []T) []Result {
		err := handler(ctx, items)
		if err == nil {
			return nil
		}
		results := make([]Result, len(items))
		for index := range results {
			results[index] = Result{State: ResultFailed, Err: err}
		}
		return results
	}
}

// PeerCanceled reports an attempted operation interrupted by the supplied handler context.
// The context must already be canceled when called. Later cancellation cannot validate a marker
// created with a live context. An independent context error should instead use ResultFailed.
func PeerCanceled(ctx context.Context) Result {
	err := ctx.Err()
	return Result{State: ResultInterrupted, Err: err, peerCanceled: err != nil}
}
