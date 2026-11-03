package durable

import (
	"fmt"
	"math"
	"time"
	"unsafe"
)

const (
	profileSQLiteWALV1   ProfileID = "sqlite-wal-v1"
	pageSize             Bytes     = 4096
	maxPayload           Bytes     = 16 << 10
	maxCodec             Bytes     = 64
	walEnvelopeBytes     Bytes     = 64 << 20
	maxCheckpointTrigger Bytes     = 16 << 20
)

// DefaultRuntime returns the installed profile's defaults for a valid durable policy.
// PROFILE.md records the tested build and finite resource qualification scope.
func DefaultRuntime(policy DurablePolicy) (RuntimeConfig, error) {
	if err := validatePolicy(policy); err != nil {
		return RuntimeConfig{}, err
	}
	return RuntimeConfig{
		Ordinary: CommandBudget{Commands: 64, Retained: 8 << 20},
		Progress: CommandBudget{Commands: 64, Retained: 256 << 10},
		Batches: BatchLimits{
			SubmitItems: 256, SubmitEncoded: 2 << 20, AckItems: 256, LookupItems: 256,
		},
		Group: GroupConfig{
			Commands: 64, Operations: 256, Encoded: 2 << 20, MaxDelay: time.Millisecond,
		},
		Delivery: DeliveryConfig{
			Items: 256, Retained: 4 << 20, ReceiveItems: 32,
			ScanRows: 256, ReadScratch: 32 << 10,
		},
		Maintenance: MaintenanceConfig{
			CleanupRows: 256, CleanupEncoded: 64 << 10, MaxCommandTurns: 8,
		},
		SQLite: SQLiteConfig{
			Cache: 64 << 20, Mmap: 256 << 20, CheckpointTrigger: maxCheckpointTrigger,
			CheckpointInterval: time.Second, BusyTimeout: 5 * time.Second,
			WALRefusal: walEnvelopeBytes,
		},
	}, nil
}

func validateProfile(profile ProfileID) error {
	if profile != profileSQLiteWALV1 {
		return fmt.Errorf("profile %q: %w", profile, ErrUnsupported)
	}
	return nil
}

func validatePolicy(policy DurablePolicy) error {
	if err := validateProfile(policy.Profile); err != nil {
		return err
	}
	valid := policy.MaxPayload > 0 && policy.MaxPayload <= maxPayload &&
		policy.MaxCodec <= maxCodec &&
		policy.MaxPendingItems > 0 && policy.MaxPendingItems <= 4096 &&
		policy.MaxPendingPayload >= policy.MaxPayload && policy.MaxPendingPayload <= 16<<20 &&
		policy.MaxReceipts >= policy.MaxPendingItems && policy.MaxReceipts <= 16384 &&
		policy.MaxProducers > 0 && policy.MaxProducers <= 256 &&
		policy.DatabaseLimit > 0 && policy.DatabaseLimit <= 1<<30 &&
		policy.DatabaseLimit%pageSize == 0
	if !valid {
		return fmt.Errorf("durable limits: %w", ErrInvalidInput)
	}
	return nil
}

func validateConfig(config Config) error {
	if err := validatePolicy(config.Policy); err != nil {
		return err
	}
	if config.Durability > ProcessCrash {
		return fmt.Errorf("durability %d: %w", config.Durability, ErrInvalidInput)
	}
	runtime := config.Runtime
	counts := []Count{
		runtime.Ordinary.Commands, runtime.Progress.Commands,
		runtime.Batches.SubmitItems, runtime.Batches.AckItems, runtime.Batches.LookupItems,
		runtime.Group.Commands, runtime.Group.Operations, runtime.Delivery.Items,
		runtime.Delivery.ReceiveItems, runtime.Delivery.ScanRows,
		runtime.Maintenance.CleanupRows, runtime.Maintenance.MaxCommandTurns,
	}
	for _, count := range counts {
		if count == 0 {
			return fmt.Errorf("runtime count: %w", ErrInvalidInput)
		}
		if _, err := allocationLength(Bytes(count)); err != nil {
			return err
		}
	}
	byteLimits := []Bytes{
		runtime.Ordinary.Retained, runtime.Progress.Retained, runtime.Batches.SubmitEncoded,
		runtime.Group.Encoded, runtime.Delivery.Retained, runtime.Delivery.ReadScratch,
		runtime.Maintenance.CleanupEncoded,
	}
	for _, limit := range byteLimits {
		if limit == 0 {
			return fmt.Errorf("runtime bytes: %w", ErrInvalidInput)
		}
	}
	if _, err := runtimeWorkingCharge(runtime); err != nil {
		return err
	}
	// V1 record limits and uint32 batch counts bound these products in Bytes.
	record := config.Policy.MaxPayload + config.Policy.MaxCodec
	maximumSubmit := min(
		runtime.Batches.SubmitEncoded,
		Bytes(runtime.Batches.SubmitItems)*(88+record),
	)
	submitRetained, err := maximumSubmitCharge(config.Policy, runtime.Batches)
	if err != nil {
		return err
	}
	lookupRetained, err := lookupCharge(Bytes(runtime.Batches.LookupItems))
	if err != nil {
		return err
	}
	deliveryRetained, err := deliveryCharge(config.Policy.MaxPayload, config.Policy.MaxCodec)
	if err != nil {
		return err
	}
	deliveryScratch, err := deliveryScratchCharge(config.Policy.MaxPayload, config.Policy.MaxCodec)
	if err != nil {
		return err
	}
	valid := runtime.Batches.SubmitEncoded >= 88+record &&
		runtime.Delivery.Retained >= deliveryRetained &&
		runtime.Delivery.ReadScratch >= deliveryScratch &&
		runtime.Ordinary.Retained >= submitRetained &&
		runtime.Ordinary.Retained >= lookupRetained &&
		runtime.Progress.Retained >= 256+128*Bytes(runtime.Batches.AckItems) &&
		runtime.Progress.Retained >= 384 &&
		runtime.Group.Operations >= max(runtime.Batches.SubmitItems, runtime.Batches.AckItems) &&
		runtime.Group.Encoded >= maximumSubmit &&
		runtime.Group.Encoded >= 56*Bytes(runtime.Batches.AckItems) &&
		runtime.Group.Encoded >= 24 &&
		runtime.Delivery.ReceiveItems <= runtime.Delivery.Items &&
		runtime.Maintenance.CleanupEncoded >= 24+32+1+8+56+26
	if !valid {
		return fmt.Errorf("runtime budget relation: %w", ErrInvalidInput)
	}
	sqlite := runtime.SQLite
	validSQLite := sqlite.Cache/1024 > 0 && sqlite.Cache/1024 <= math.MaxInt32 &&
		sqlite.Mmap <= math.MaxInt64 && sqlite.CheckpointTrigger > 0 &&
		sqlite.CheckpointTrigger < sqlite.WALRefusal && sqlite.WALRefusal <= math.MaxInt64 &&
		sqlite.CheckpointInterval > 0 && sqlite.BusyTimeout >= 0 &&
		sqlite.BusyTimeout/time.Millisecond <= math.MaxInt32 && runtime.Group.MaxDelay >= 0
	if !validSQLite {
		return fmt.Errorf("SQLite tuning: %w", ErrInvalidInput)
	}
	return nil
}

// normalizeConfig resolves the one compatibility default before storing runtime settings.
func normalizeConfig(config Config) Config {
	if config.Runtime.SQLite.WALRefusal == 0 {
		config.Runtime.SQLite.WALRefusal = walEnvelopeBytes
	}
	return config
}

func deliveryRingCapacity(items Count) (int, error) {
	// The frontier ring accepts powers of two through 2^30.
	if items == 0 || items > 1<<30 {
		return 0, ErrInvalidInput
	}
	capacity := Bytes(1)
	for capacity < Bytes(items) {
		capacity *= 2
	}
	return allocationLength(capacity)
}

// runtimeWorkingCharge covers configuration-sized arrays, including the rounded ring.
// Caller-owned results, diagnostics, and SQLite native memory remain separate.
func runtimeWorkingCharge(runtime RuntimeConfig) (Bytes, error) {
	capacity, err := deliveryRingCapacity(runtime.Delivery.Items)
	if err != nil {
		return 0, err
	}
	arrays := []struct{ count, size Bytes }{
		{Bytes(runtime.Ordinary.Commands), Bytes(unsafe.Sizeof(command{}))},
		{Bytes(runtime.Progress.Commands), Bytes(unsafe.Sizeof(command{}))},
		{Bytes(runtime.Delivery.Items), Bytes(unsafe.Sizeof(claim{}))},
		{Bytes(capacity), Bytes(unsafe.Sizeof(int(0)))},
		{Bytes(capacity), 8}, {Bytes(capacity), 8},
		{Bytes(runtime.Group.Commands), Bytes(unsafe.Sizeof(groupedCommand{}))},
	}
	var working Bytes
	for _, array := range arrays {
		size, err := checkedMultiply(array.count, array.size)
		if err != nil {
			return 0, err
		}
		charge, err := allocationCharge(size)
		if err != nil {
			return 0, err
		}
		working, err = checkedAdd(working, charge)
		if err != nil {
			return 0, err
		}
	}
	// Check public-batch arrays even where their reservation has a separate formula.
	batches := []struct{ count, size Bytes }{
		{Bytes(runtime.Batches.SubmitItems), Bytes(unsafe.Sizeof(Entry{}))},
		{Bytes(runtime.Batches.SubmitItems), 1},
		{Bytes(runtime.Batches.SubmitItems), 16},
		{Bytes(runtime.Batches.AckItems), Bytes(unsafe.Sizeof(Token{}))},
		{Bytes(runtime.Batches.LookupItems), Bytes(unsafe.Sizeof(ID{}))},
		{Bytes(runtime.Batches.LookupItems), Bytes(unsafe.Sizeof(Status{}))},
		{Bytes(runtime.Delivery.ReceiveItems), Bytes(unsafe.Sizeof(Delivery{}))},
	}
	for _, array := range batches {
		size, err := checkedMultiply(array.count, array.size)
		if err != nil {
			return 0, err
		}
		if _, err := allocationCharge(size); err != nil {
			return 0, err
		}
	}
	total := working
	for _, budget := range []Bytes{runtime.Ordinary.Retained, runtime.Progress.Retained,
		runtime.Delivery.Retained, runtime.Delivery.ReadScratch} {
		total, err = checkedAdd(total, budget)
		if err != nil {
			return 0, err
		}
	}
	return working, nil
}
