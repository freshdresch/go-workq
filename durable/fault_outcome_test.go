//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

// Each child owns the process-wide fixture until the real coordinator joins.
func TestFaultOutcomes(t *testing.T) {
	if mode := os.Getenv("WORKQ_OUTCOME_MODE"); mode != "" {
		runFaultOutcome(t, mode)
		return
	}
	for _, mode := range []string{
		"statement", "rollback", "commit-full", "commit-short", "commit-sync-before",
		"commit-sync-after", "ack-sync", "close-sync", "close-truncate", "abort",
		"close-hidden", "close-owned",
	} {
		t.Run(mode, func(t *testing.T) {
			base := os.Getenv("WORKQ_OUTCOME_EVIDENCE")
			if base == "" {
				base = t.TempDir()
			}
			evidence, err := os.MkdirTemp(base, mode+"-")
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultOutcomes$", "-test.v")
			child.Env = append(os.Environ(), "WORKQ_OUTCOME_MODE="+mode,
				"WORKQ_OUTCOME_DIRECTORY="+evidence)
			output, err := child.CombinedOutput()
			require.NoError(t, os.WriteFile(filepath.Join(evidence, "child.log"), output, 0600))
			require.NoError(t, err, "evidence %s\n%s", evidence, output)
			t.Logf("evidence %s", evidence)
		})
	}
}

type faultOutcomeObserver struct {
	queue            *Queue
	controller       *sqlitefault.Controller
	mode             string
	entered, release chan struct{}
	setupErr         error
	autoCommit       bool
	rollbackDenied   bool
	checkpoints      int
}

func (o *faultOutcomeObserver) BeforeExecute() {
	close(o.entered)
	<-o.release
	o.setupErr = o.queue.opened.storage.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		if !ok {
			return ErrStorage
		}
		connection.RegisterAuthorizer(func(action int, name, _, _ string) int {
			if action == sqlite3.SQLITE_PRAGMA {
				if name == "page_count" {
					o.autoCommit = connection.AutoCommit()
				}
				if name == "wal_checkpoint" {
					o.checkpoints++
				}
			}
			if (o.mode == "statement" || o.mode == "rollback") &&
				action == sqlite3.SQLITE_INSERT && name == "payloads" {
				return sqlite3.SQLITE_DENY
			}
			if o.mode == "rollback" && action == sqlite3.SQLITE_TRANSACTION && name == "ROLLBACK" {
				o.rollbackDenied = true
				return sqlite3.SQLITE_DENY
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	})
	if o.setupErr != nil {
		return
	}
	switch o.mode {
	case "commit-full":
		o.setupErr = o.controller.Arm("native-write", "wal", 1, "before", sqlitefault.FullError)
	case "commit-short":
		o.setupErr = o.controller.Arm("native-write", "wal", 1, "before", sqlitefault.ShortWrite(8))
	case "commit-sync-before", "ack-sync":
		o.setupErr = o.controller.Arm("native-sync", "wal", 1, "before", sqlitefault.SyncError)
	case "commit-sync-after":
		o.setupErr = o.controller.Arm("native-sync", "wal", 1, "after", sqlitefault.SyncError)
	case "close-sync":
		o.setupErr = o.controller.Arm("native-sync", "main", 1, "before", sqlitefault.SyncError)
	case "close-truncate":
		o.setupErr = o.controller.Arm("native-truncate", "wal", 1, "before", sqlitefault.IOError)
	case "close-hidden":
		o.setupErr = o.controller.Arm("native-close", "main", 1, "after", sqlitefault.CloseError)
	case "close-owned":
		// Closing a real owned descriptor makes the later Close return os.ErrClosed.
		o.setupErr = o.queue.opened.owner.parent.Close()
	}
}

type faultOutcomeResult struct {
	result CommitResult
	err    error
}

type faultOutcomeEvidence struct {
	Mode                string              `json:"mode"`
	Decision            Decision            `json:"decision"`
	AutoCommit          bool                `json:"auto_commit"`
	RollbackDenied      bool                `json:"rollback_denied"`
	ExplicitCheckpoints int                 `json:"explicit_checkpoints"`
	Events              []sqlitefault.Event `json:"events"`
	Recovered           Inspection          `json:"recovered"`
}

func runFaultOutcome(t *testing.T, mode string) {
	evidence := os.Getenv("WORKQ_OUTCOME_DIRECTORY")
	root := filepath.Join(evidence, "queue")
	require.NoError(t, os.Mkdir(root, 0700))
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 8192})
	require.NoError(t, err)
	faultController.Store(controller)
	t.Cleanup(func() {
		faultController.Store(nil)
		assert.NoError(t, controller.Close())
	})
	config := testConfig(t)
	config.Runtime.Group.Commands = 1
	config.Runtime.Group.MaxDelay = 0
	queue, err := Open(t.Context(), root, config)
	require.NoError(t, err)
	t.Cleanup(func() { queue.Abort(ErrStopped); _ = queue.Wait() })
	entries := []Entry{{ID: ID{Sequence: 1}, Payload: []byte("first")},
		{ID: ID{Sequence: 2}, Payload: []byte("second")}}
	shutdown := mode == "close-sync" || mode == "close-truncate" || mode == "abort" ||
		mode == "close-hidden" || mode == "close-owned"
	var tokens []Token
	if shutdown || mode == "ack-sync" {
		result, err := queue.SubmitBatch(t.Context(), entries)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
		if mode == "ack-sync" {
			tokens = receiveCrashTokens(t, queue, 2)
		}
	}
	observer := &faultOutcomeObserver{queue: queue, controller: controller, mode: mode,
		entered: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(observer.release) })
	t.Cleanup(unblock)
	resultReady := make(chan faultOutcomeResult, 1)
	go func() {
		var result CommitResult
		var err error
		if mode == "ack-sync" {
			result, err = queue.ackBatchObserved(t.Context(), tokens, observer)
		} else {
			batch := entries
			if shutdown {
				batch = nil
			}
			result, err = queue.submitBatchObserved(t.Context(), batch, observer)
		}
		resultReady <- faultOutcomeResult{result: result, err: err}
	}()
	<-observer.entered
	queue.mu.Lock()
	sequence := queue.lastSequence
	queue.mu.Unlock()
	unstarted := make(chan faultOutcomeResult, 1)
	if !shutdown {
		go func() {
			result, err := queue.SubmitBatch(t.Context(), []Entry{{ID: ID{Sequence: 3}}})
			unstarted <- faultOutcomeResult{result: result, err: err}
		}()
		waitQueueSequence(t, queue, sequence+1)
	}
	closeReady := make(chan error, 1)
	abortCause := errors.New("outcome test abort")
	if shutdown {
		if mode == "abort" {
			queue.Abort(abortCause)
		} else {
			go func() { closeReady <- queue.Close(t.Context()) }()
			<-queue.Stopping()
		}
	}
	unblock()
	outcome := <-resultReady
	joined := queue.Wait()
	require.NoError(t, observer.setupErr)
	assert.Equal(t, Closed, queue.Snapshot().State)
	wantFault := mode != "abort" && mode != "close-hidden"
	assert.Equal(t, wantFault, queue.faulted)
	assert.Equal(t, wantFault, queue.Snapshot().Cause.Storage)
	for _, file := range []*os.File{queue.opened.main, queue.opened.owner.parent,
		queue.opened.owner.directory, queue.opened.owner.lock} {
		_, err := file.Stat()
		assert.ErrorIs(t, err, os.ErrClosed, "owned descriptor survived join")
	}
	select {
	case <-queue.Done():
	default:
		t.Error("Wait returned before Done")
	}
	if shutdown {
		require.NoError(t, outcome.err)
		assert.Equal(t, Applied, outcome.result.Decision)
		if mode == "abort" {
			assert.ErrorIs(t, joined, abortCause)
			assert.Zero(t, observer.checkpoints)
		} else {
			closeErr := <-closeReady
			assert.Equal(t, 1, observer.checkpoints)
			if mode == "close-hidden" {
				assert.NoError(t, closeErr)
				assert.NoError(t, joined)
			} else {
				assert.ErrorIs(t, closeErr, ErrStorage)
				assert.ErrorIs(t, joined, ErrStorage)
			}
			if mode == "close-owned" {
				assert.ErrorIs(t, joined, os.ErrClosed)
			}
			if mode == "close-sync" || mode == "close-truncate" {
				var native sqlite3.Error
				require.ErrorAs(t, joined, &native)
				expected := sqlite3.ErrIoErrFsync
				if mode == "close-truncate" {
					expected = sqlite3.ErrNoExtended(sqlite3.ErrIoErr)
				}
				assert.Equal(t, expected, native.ExtendedCode)
			}
		}
	} else {
		assert.ErrorIs(t, outcome.err, ErrStorage)
		assert.ErrorIs(t, joined, ErrStorage)
		want := Unknown
		if mode == "statement" {
			want = NotApplied
		}
		assert.Equal(t, want, outcome.result.Decision)
		var native sqlite3.Error
		require.ErrorAs(t, outcome.err, &native)
		expectedCode := sqlite3.ErrIoErrFsync
		switch mode {
		case "statement", "rollback":
			expectedCode = sqlite3.ErrNoExtended(sqlite3.ErrAuth)
		case "commit-full":
			expectedCode = sqlite3.ErrNoExtended(sqlite3.ErrFull)
		case "commit-short":
			expectedCode = sqlite3.ErrIoErrWrite
		}
		assert.Equal(t, expectedCode, native.ExtendedCode)
		later := <-unstarted
		assert.Equal(t, NotApplied, later.result.Decision)
		assert.ErrorIs(t, later.err, ErrStopped)
		assert.ErrorIs(t, later.err, ErrStorage)
		statuses, err := queue.Lookup(t.Context(), []ID{entries[0].ID})
		assert.Nil(t, statuses)
		assert.ErrorIs(t, err, ErrStorage)
		deliveries, err := queue.Receive(t.Context(), 1)
		assert.Empty(t, deliveries)
		assert.ErrorIs(t, err, ErrStorage)
		if mode == "ack-sync" {
			assert.ErrorIs(t, queue.Release(tokens[0]), ErrStorage)
		}
		assert.Zero(t, observer.checkpoints, "fault must not start a shutdown checkpoint")
		if mode == "rollback" {
			assert.True(t, observer.rollbackDenied)
			assert.Equal(t, 2, countNativeAuthErrors(outcome.err),
				"command result must retain INSERT and ROLLBACK authorization errors")
			assert.Equal(t, 2, countNativeAuthErrors(joined),
				"Wait must retain INSERT and ROLLBACK authorization errors")
		} else {
			assert.True(t, observer.autoCommit, "ended transaction cannot establish noncommit")
		}
	}
	events := controller.Events()
	var injected []sqlitefault.Event
	for _, event := range events {
		if event.Injected {
			injected = append(injected, event)
		}
	}
	if mode != "statement" && mode != "rollback" && mode != "abort" && mode != "close-owned" {
		require.Len(t, injected, 1)
		assert.NotZero(t, injected[0].Result)
		assert.Equal(t, injected[0].Expected, injected[0].Actual)
	}
	require.False(t, controller.Armed())
	require.NoError(t, controller.Err())
	faultController.Store(nil)
	ids := []ID{entries[0].ID, entries[1].ID, {Sequence: 3}}
	recovered, err := Inspect(t.Context(), root, ids)
	require.NoError(t, err, "join released permanent ownership and left a valid store")
	require.Len(t, recovered.Statuses, 3)
	assert.Equal(t, recovered.Statuses[0].Kind, recovered.Statuses[1].Kind,
		"public batch is atomic after recovery")
	assert.Equal(t, Absent, recovered.Statuses[2].Kind, "unstarted command never committed")
	if shutdown {
		assert.Equal(t, Pending, recovered.Statuses[0].Kind)
	}
	if mode == "statement" || mode == "rollback" || mode == "commit-full" || mode == "commit-short" {
		assert.Equal(t, Absent, recovered.Statuses[0].Kind)
	}
	if mode == "commit-sync-before" || mode == "commit-sync-after" {
		assert.Contains(t, []StatusKind{Absent, Pending}, recovered.Statuses[0].Kind)
	}
	if mode == "ack-sync" {
		assert.Contains(t, []StatusKind{Pending, Acknowledged}, recovered.Statuses[0].Kind)
	}
	record := faultOutcomeEvidence{Mode: mode, Decision: outcome.result.Decision,
		AutoCommit: observer.autoCommit, RollbackDenied: observer.rollbackDenied,
		ExplicitCheckpoints: observer.checkpoints, Events: events, Recovered: recovered}
	data, err := json.MarshalIndent(record, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(evidence, "outcome.json"), data, 0600))
}

func countNativeAuthErrors(err error) int {
	switch current := err.(type) {
	case interface{ Unwrap() []error }:
		var count int
		for _, child := range current.Unwrap() {
			count += countNativeAuthErrors(child)
		}
		return count
	case interface{ Unwrap() error }:
		return countNativeAuthErrors(current.Unwrap())
	case sqlite3.Error:
		if current.Code == sqlite3.ErrAuth {
			return 1
		}
	}
	return 0
}
