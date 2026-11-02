package durable

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPolicy() DurablePolicy {
	return DurablePolicy{
		Profile: profileSQLiteWALV1, MaxPayload: 16 << 10, MaxCodec: 64,
		MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
		MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
	}
}

func testConfig(t *testing.T) Config {
	t.Helper()
	policy := testPolicy()
	runtime, err := DefaultRuntime(policy)
	require.NoError(t, err)
	return Config{Policy: policy, Runtime: runtime}
}

func TestConfigRelationsBeforeOpening(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"profile", func(cfg *Config) { cfg.Policy.Profile = "unknown" }},
		{"durability", func(cfg *Config) { cfg.Durability = 2 }},
		{"pending receipts", func(cfg *Config) { cfg.Policy.MaxReceipts = 4095 }},
		{"pending bytes", func(cfg *Config) { cfg.Policy.MaxPendingPayload = 1 }},
		{"database alignment", func(cfg *Config) { cfg.Policy.DatabaseLimit-- }},
		{"overflow bytes", func(cfg *Config) { cfg.Policy.MaxPayload = math.MaxUint64 }},
		{"overflow count", func(cfg *Config) { cfg.Runtime.Batches.AckItems = math.MaxUint32 }},
		{"submit record", func(cfg *Config) { cfg.Runtime.Batches.SubmitEncoded = 1 }},
		{"ordinary submit", func(cfg *Config) { cfg.Runtime.Ordinary.Retained = 2 << 20 }},
		{"ordinary lookup", func(cfg *Config) {
			cfg.Runtime.Batches.SubmitItems = 1
			cfg.Runtime.Batches.SubmitEncoded = 17000
			cfg.Runtime.Ordinary.Retained = 18000
		}},
		{"progress ack", func(cfg *Config) { cfg.Runtime.Progress.Retained = 32768 }},
		{"progress control", func(cfg *Config) {
			cfg.Runtime.Batches.AckItems = 1
			cfg.Runtime.Progress.Retained = 383
		}},
		{"group operations", func(cfg *Config) { cfg.Runtime.Group.Operations = 255 }},
		{"group bytes", func(cfg *Config) { cfg.Runtime.Group.Encoded = 1 << 20 }},
		{"delivery record", func(cfg *Config) { cfg.Runtime.Delivery.Retained = 16384 }},
		{"read scratch", func(cfg *Config) { cfg.Runtime.Delivery.ReadScratch = 16384 }},
		{"receive count", func(cfg *Config) { cfg.Runtime.Delivery.Items = 31 }},
		{"cleanup record", func(cfg *Config) { cfg.Runtime.Maintenance.CleanupEncoded = 1 }},
		{"zero service", func(cfg *Config) { cfg.Runtime.Maintenance.MaxCommandTurns = 0 }},
		{"cache", func(cfg *Config) { cfg.Runtime.SQLite.Cache = 1 }},
		{"mmap", func(cfg *Config) { cfg.Runtime.SQLite.Mmap = math.MaxUint64 }},
		{"trigger", func(cfg *Config) { cfg.Runtime.SQLite.CheckpointTrigger = 0 }},
		{"interval", func(cfg *Config) { cfg.Runtime.SQLite.CheckpointInterval = 0 }},
		{"busy timeout", func(cfg *Config) { cfg.Runtime.SQLite.BusyTimeout = -1 }},
		{"group delay", func(cfg *Config) { cfg.Runtime.Group.MaxDelay = 11 * time.Millisecond }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			tc.change(&cfg)
			path := filepath.Join(t.TempDir(), "absent.sqlite3")
			storage, err := connectStore(t.Context(), path, cfg, true)
			require.Error(t, err)
			assert.Nil(t, storage)
			_, err = os.Stat(path)
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestRuntimeDefaultsAndExplicitZeros(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	assert.NoError(t, validateConfig(cfg))
	cfg.Runtime.SQLite.Mmap = 0
	cfg.Runtime.SQLite.BusyTimeout = 0
	cfg.Runtime.Group.MaxDelay = 0
	assert.NoError(t, validateConfig(cfg))
	assert.ErrorIs(t, validateConfig(Config{}), ErrUnsupported)
	runtime, err := DefaultRuntime(DurablePolicy{})
	assert.ErrorIs(t, err, ErrUnsupported)
	assert.Equal(t, RuntimeConfig{}, runtime)
}

func TestRetainedBudgetBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		payload    Bytes
		codec      Bytes
		items      Count
		encoded    Bytes
		lookup     Count
		wantSubmit Bytes
	}{
		{"defaults", 16384, 64, 256, 2 << 20, 256, 4223872},
		{"single8193", 8193, 0, 1, 8281, 1, 16832},
		{"noCodec", 16384, 0, 256, 2 << 20, 256, 4192768},
		{"tinyPayload", 1, 0, 256, 22784, 1, 42240},
		{"encodedLimited", 1, 0, 256, 89, 1, 448},
		{"lookupMaximum", 1, 0, 1, 89, 256, 448},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := testConfig(t)
			config.Policy.MaxPayload, config.Policy.MaxCodec = tc.payload, tc.codec
			config.Runtime.Batches.SubmitItems = tc.items
			config.Runtime.Batches.SubmitEncoded = tc.encoded
			config.Runtime.Batches.LookupItems = tc.lookup
			maximum, err := maximumSubmitCharge(config.Policy, config.Runtime.Batches)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSubmit, maximum)
			lookup, err := lookupCharge(Bytes(tc.lookup))
			require.NoError(t, err)
			budget := max(maximum, lookup)
			config.Runtime.Ordinary.Retained = budget
			require.NoError(t, validateConfig(config))
			queue := &Queue{config: config}
			payloadBuffer := make([]byte, int(tc.payload))
			for count := 0; count <= int(tc.items); count++ {
				if 88*Bytes(count) > tc.encoded {
					break
				}
				entries := make([]Entry, count)
				remaining := tc.encoded - 88*Bytes(count)
				for index := range entries {
					payload := min(tc.payload, remaining)
					entries[index] = Entry{ID: ID{Sequence: uint64(index)},
						Payload: payloadBuffer[:int(payload)]}
					remaining -= payload
				}
				allocation, err := queue.validateSubmit(entries)
				require.NoError(t, err)
				assert.LessOrEqual(t, allocation.charge, budget)
			}
			config.Runtime.Ordinary.Retained--
			directory := filepath.Join(t.TempDir(), "absent")
			queue, err = Open(t.Context(), directory, config)
			assert.Nil(t, queue)
			assert.ErrorIs(t, err, ErrInvalidInput)
			_, err = os.Stat(directory)
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestConfigExactRelationBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(*Config)
		lower func(*Config)
	}{
		{"maximum record", func(cfg *Config) {
			cfg.Runtime.Batches.SubmitEncoded = 88 + 16384 + 64
		}, func(cfg *Config) { cfg.Runtime.Batches.SubmitEncoded-- }},
		{"delivery record", func(cfg *Config) {
			cfg.Runtime.Delivery.Retained = 33152
		}, func(cfg *Config) { cfg.Runtime.Delivery.Retained-- }},
		{"read scratch", func(cfg *Config) {
			cfg.Runtime.Delivery.ReadScratch = 17664
		}, func(cfg *Config) { cfg.Runtime.Delivery.ReadScratch-- }},
		{"progress ack", func(cfg *Config) {
			cfg.Runtime.Progress.Retained = 33024
		}, func(cfg *Config) { cfg.Runtime.Progress.Retained-- }},
		{"progress control", func(cfg *Config) {
			cfg.Runtime.Batches.AckItems = 1
			cfg.Runtime.Progress.Retained = 384
		}, func(cfg *Config) { cfg.Runtime.Progress.Retained-- }},
		{"group ack bytes", func(cfg *Config) {
			cfg.Policy.MaxPayload, cfg.Policy.MaxCodec = 1, 0
			cfg.Runtime.Batches.SubmitItems = 1
			cfg.Runtime.Batches.SubmitEncoded = 89
			cfg.Runtime.Group.Encoded = 56 * 256
		}, func(cfg *Config) { cfg.Runtime.Group.Encoded-- }},
		{"cleanup record", func(cfg *Config) {
			cfg.Runtime.Maintenance.CleanupEncoded = 147
		}, func(cfg *Config) { cfg.Runtime.Maintenance.CleanupEncoded-- }},
		{"receive items", func(cfg *Config) {
			cfg.Runtime.Delivery.ReceiveItems = 256
		}, func(cfg *Config) { cfg.Runtime.Delivery.Items-- }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			tc.setup(&cfg)
			require.NoError(t, validateConfig(cfg))
			tc.lower(&cfg)
			assert.ErrorIs(t, validateConfig(cfg), ErrInvalidInput)
		})
	}
}

func TestCheckedArithmeticExactLimits(t *testing.T) {
	t.Parallel()
	sum, err := checkedAdd(math.MaxUint64-1, 1)
	require.NoError(t, err)
	assert.Equal(t, Bytes(math.MaxUint64), sum)
	product, err := checkedMultiply(math.MaxUint64, 1)
	require.NoError(t, err)
	assert.Equal(t, Bytes(math.MaxUint64), product)
	product, err = checkedMultiply(math.MaxUint64, 0)
	require.NoError(t, err)
	assert.Zero(t, product)
	_, err = allocationCharge(Bytes(math.MaxInt))
	assert.ErrorIs(t, err, ErrInvalidInput)
	charge, err := allocationCharge(Bytes(math.MaxInt) - 7)
	require.NoError(t, err)
	assert.Equal(t, Bytes(math.MaxUint64)-15, charge)
}
