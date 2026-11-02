// Package durable defines the storage foundation for a bounded SQLite WAL work queue.
package durable

import (
	"strconv"
	"time"
)

// Bytes is a byte count, independent of SQLite page allocation.
type Bytes uint64

// Count is a bounded logical item count.
type Count uint32

// ProfileID identifies a recognized storage and accounting format.
type ProfileID string

// Durability selects the persistence guarantee for ordinary transactions.
type Durability uint8

const (
	// Synchronized uses SQLite FULL synchronization.
	Synchronized Durability = iota
	// ProcessCrash uses NORMAL and permits recent state loss after a system crash.
	ProcessCrash
)

// String returns the durability name, or Durability(n) for an undeclared value n.
func (d Durability) String() string {
	switch d {
	case Synchronized:
		return "synchronized"
	case ProcessCrash:
		return "process-crash"
	default:
		return "Durability(" + strconv.Itoa(int(d)) + ")"
	}
}

// ID is a stable producer occurrence. Every producer and sequence value is legal.
type ID struct {
	Producer [16]byte
	Sequence uint64
}

// UUID identifies one initialized store across sessions.
type UUID [16]byte

// Token is an opaque, comparable delivery fence. Its zero value is invalid.
type Token struct {
	bytes [56]byte
}

// Entry is application-encoded work. Callers retain ownership of its storage.
type Entry struct {
	ID      ID
	Codec   string
	Payload []byte
}

// DurablePolicy contains immutable logical limits and the main database cap.
//
//nolint:revive // DurablePolicy is the agreed public API name.
type DurablePolicy struct {
	Profile           ProfileID
	MaxPayload        Bytes
	MaxCodec          Bytes
	MaxPendingItems   Count
	MaxPendingPayload Bytes
	MaxReceipts       Count
	MaxProducers      Count
	DatabaseLimit     Bytes
}

// Config separates persistent policy from per-session resources.
// The zero Config is invalid. Runtime zero fields do not request defaults.
type Config struct {
	Policy     DurablePolicy
	Durability Durability
	Runtime    RuntimeConfig
}

// RuntimeConfig bounds independently reserved work and SQLite tuning.
type RuntimeConfig struct {
	Ordinary    CommandBudget
	Progress    CommandBudget
	Batches     BatchLimits
	Group       GroupConfig
	Delivery    DeliveryConfig
	Maintenance MaintenanceConfig
	SQLite      SQLiteConfig
}

// CommandBudget bounds outstanding commands and their retained inputs/results.
type CommandBudget struct {
	Commands Count
	Retained Bytes
}

// BatchLimits bounds complete public commands, including duplicate entries.
type BatchLimits struct {
	SubmitItems   Count
	SubmitEncoded Bytes
	AckItems      Count
	LookupItems   Count
}

// GroupConfig bounds complete commands in one transaction.
type GroupConfig struct {
	Commands   Count
	Operations Count
	Encoded    Bytes
	MaxDelay   time.Duration
}

// DeliveryConfig bounds claims, receive descriptors, and one read scratch buffer.
type DeliveryConfig struct {
	Items        Count
	Retained     Bytes
	ReceiveItems Count
	ScanRows     Count
	ReadScratch  Bytes
}

// MaintenanceConfig bounds receipt cleanup work and its service interval.
type MaintenanceConfig struct {
	CleanupRows     Count
	CleanupEncoded  Bytes
	MaxCommandTurns Count
}

// SQLiteConfig requests finite pager resources and checkpoint scheduling.
// Mmap, BusyTimeout, and GroupConfig.MaxDelay accept explicit zero values.
// Cache rounds down to whole KiB and BusyTimeout to whole milliseconds in SQLite.
type SQLiteConfig struct {
	Cache              Bytes
	Mmap               Bytes
	CheckpointTrigger  Bytes
	CheckpointInterval time.Duration
	BusyTimeout        time.Duration
}

// StoreCounts contains the six transactionally maintained logical counts.
type StoreCounts struct {
	PendingItems     Count
	PendingPayload   Bytes
	Receipts         Count
	Producers        Count
	RetiredProducers Count
	CleanupMarkers   Count
}
