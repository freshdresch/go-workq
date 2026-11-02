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
// [ChangeDurability]. The sqlite-wal-v1 profile is a candidate awaiting resource and fault
// qualification, not a qualified production deployment envelope.
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
// The implementation requires cgo and a C compiler with pinned bundled SQLite. Its support
// scope is Linux amd64 on local ext4 with private directory ownership and storage that honors
// synchronization. Network filesystems and Windows-mounted WSL paths are outside that scope.
package durable
