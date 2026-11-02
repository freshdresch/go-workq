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
