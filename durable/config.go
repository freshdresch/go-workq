package durable

import (
	"fmt"
	"time"
)

const (
	profileSQLiteWALV1 ProfileID = "sqlite-wal-v1"
	pageSize           Bytes     = 4096
	maxPayload         Bytes     = 16 << 10
	maxCodec           Bytes     = 64
)

// DefaultRuntime returns the candidate profile's defaults for a valid durable policy.
// The profile remains subject to production resource qualification.
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
			Cache: 64 << 20, Mmap: 256 << 20, CheckpointTrigger: 16 << 20,
			CheckpointInterval: time.Second, BusyTimeout: 5 * time.Second,
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
	counts := []struct{ value, ceiling Count }{
		{runtime.Ordinary.Commands, 64}, {runtime.Progress.Commands, 64},
		{runtime.Batches.SubmitItems, 256}, {runtime.Batches.AckItems, 256},
		{runtime.Batches.LookupItems, 256}, {runtime.Group.Commands, 64},
		{runtime.Group.Operations, 256}, {runtime.Delivery.Items, 256},
		{runtime.Delivery.ReceiveItems, 256}, {runtime.Delivery.ScanRows, 256},
		{runtime.Maintenance.CleanupRows, 256}, {runtime.Maintenance.MaxCommandTurns, 8},
	}
	for _, limit := range counts {
		if limit.value == 0 || limit.value > limit.ceiling {
			return fmt.Errorf("runtime count: %w", ErrInvalidInput)
		}
	}
	byteLimits := []struct{ value, ceiling Bytes }{
		{runtime.Ordinary.Retained, 8 << 20}, {runtime.Progress.Retained, 256 << 10},
		{runtime.Batches.SubmitEncoded, 2 << 20}, {runtime.Group.Encoded, 2 << 20},
		{runtime.Delivery.Retained, 4 << 20}, {runtime.Delivery.ReadScratch, 32 << 10},
		{runtime.Maintenance.CleanupEncoded, 64 << 10},
	}
	for _, limit := range byteLimits {
		if limit.value == 0 || limit.value > limit.ceiling {
			return fmt.Errorf("runtime bytes: %w", ErrInvalidInput)
		}
	}
	// Ceilings above make all subsequent sums, products, and signed conversions safe.
	record := config.Policy.MaxPayload + config.Policy.MaxCodec
	maximumSubmit := min(
		runtime.Batches.SubmitEncoded,
		Bytes(runtime.Batches.SubmitItems)*(88+record),
	)
	valid := runtime.Batches.SubmitEncoded >= 88+record &&
		runtime.Delivery.Retained >= record+256 && runtime.Delivery.ReadScratch >= record &&
		runtime.Ordinary.Retained >= 256+maximumSubmit+128*Bytes(runtime.Batches.SubmitItems) &&
		runtime.Ordinary.Retained >= 256+128*Bytes(runtime.Batches.LookupItems) &&
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
	validSQLite := sqlite.Cache >= 8<<20 && sqlite.Cache <= 64<<20 &&
		sqlite.Mmap <= 256<<20 && sqlite.CheckpointTrigger >= 4<<20 &&
		sqlite.CheckpointTrigger <= 16<<20 && sqlite.CheckpointInterval > 0 &&
		sqlite.CheckpointInterval <= time.Second && sqlite.BusyTimeout >= 0 &&
		sqlite.BusyTimeout <= 5*time.Second &&
		runtime.Group.MaxDelay >= 0 && runtime.Group.MaxDelay <= 10*time.Millisecond
	if !validSQLite {
		return fmt.Errorf("SQLite tuning: %w", ErrInvalidInput)
	}
	return nil
}
