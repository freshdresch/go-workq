// Package durable stores bounded work in SQLite WAL for delivery across process restarts.
// Applications own encoding, workers, retries, and external effects. The queue supplies
// stable occurrence IDs, atomic submission and acknowledgement batches, and fenced attempts.
// It does not provide exactly-once external effects.
//
// Configure explicit [DurablePolicy] limits, then obtain runtime settings with
// [DefaultRuntime]. The zero [Config] is invalid. [Synchronized], the default durability,
// uses SQLite FULL. [ProcessCrash] uses NORMAL, which permits recent successful submissions,
// acknowledgements, and retirement floors to roll back after OS crash or power loss.
// Open requires matching persisted policy. Change durability only while offline with
// [ChangeDurability]. The sqlite-wal-v1 profile has a 64 MiB operational WAL envelope.
// An observation above it irreversibly stops mutations and new delivery for that session.
// A crossing commit keeps its Applied result. Healthy Lookup and Release remain available.
// The envelope is not a hard file-size cap. See PROFILE.md for measured resource evidence
// and remaining release qualification.
//
// Keep the same [ID], codec, and payload when retrying a source occurrence. Inputs remain
// caller-owned and must stay unchanged during a call. The queue copies admitted input, so
// callers may reuse it after return, including after [Unknown]. An Unknown mutation may
// still execute or may have committed. Reconcile IDs through healthy ordered [Queue.Lookup],
// or stop and join workers, close the queue, and use [Inspect]. An error never means Absent.
//
// [Queue.Receive] transfers detached bytes and one attempt per ID. Ack only established
// external success. On an uncertain Ack, reconcile before repeating the effect. There are
// no visibility leases or automatic retry timers. Stop using an attempt before [Queue.Release]
// permits redelivery. Tokens fence queue operations, not external effects in old workers.
//
// Stop sources and admission to drain. Keep Ack service alive until workers and their
// terminal callbacks finish, then close the queue. For an interrupted service, stop delivery,
// cancel and join workers, then close. [Queue.Close] does not complete pending work. Its
// deadline requests abort and still joins resources. Stuck kernel I/O can delay return.
// Closure reports explicit synchronization failures and errors returned by owned resources.
// SQLite suppresses some native destructor errors. Cancellation can also hide a concurrent
// SQL failure inside the driver, so cancellation alone does not establish storage health.
//
// Acknowledged receipts consume capacity until [Queue.RetireThrough] and bounded cleanup.
// Retirement also retires holes and must be coordinated with source retries. Producers
// consume lifetime capacity. [Maintain] reclaims available physical pages offline, without
// removing logical obligations or promising future Ack capacity. Preserve the database and
// surviving WAL together. Never delete WAL to recover from an error.
//
// Bound concurrent callers separately from command and delivery budgets. Waiting calls can
// retain old queue-created notification channels, plus caller stacks, contexts, and runtime
// wait state. Those live objects are outside the fixed queue-retained budgets. Queue counters
// do not bound total process memory or caller-retained deliveries.
//
// Caller-created input, configuration, and error graphs retained through references also need
// application provision. Abort retains its full cause for Wait and errors.Is/errors.As.
// Error-reporting graphs and formatting are outside working-storage budgets, including
// queue-created diagnostic message copies. Fault/Abort reporting needs application headroom.
// Snapshot retains at most 256 detail bytes, but preparing that detail can allocate more.
//
// Open and offline calls limit directory input, absolute form, and resolved form to
// 4095 bytes each. Longer paths return [ErrInvalidInput]. Relative and symlink aliases
// remain supported. Filesystem normalization and resolution temporaries, including those
// allocated by queue-invoked library calls, need separate startup/backend memory provision.
// This phase precedes logical validation and active pools. Symlink traversal scratch is
// not bounded by the input limit. Retained canonical paths and DSNs count as fixed storage.
//
// The implementation requires cgo and a C compiler with pinned bundled SQLite. Its support
// scope is Linux amd64 on local ext4 with private directory ownership and storage that honors
// synchronization. Network filesystems and Windows-mounted WSL paths are outside that scope.
package durable
