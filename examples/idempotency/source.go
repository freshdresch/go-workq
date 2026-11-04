package main

import (
	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

const sourceNamespace = "workq-idempotency-demo-v1"

func sourceEntries() []durable.Entry {
	// These checked-in identities and bytes are the demo's durable source intent.
	producer := [16]byte{
		'w', 'o', 'r', 'k', 'q', '-', 'i', 'd', '-', 'd', 'e', 'm', 'o', '-', 'v', '1',
	}
	return []durable.Entry{
		{ID: durable.ID{Producer: producer, Sequence: 1}, Codec: inventory.Codec,
			Payload: []byte(`{"item":"widget","delta":3}`)},
		{ID: durable.ID{Producer: producer, Sequence: 2}, Codec: inventory.Codec,
			Payload: []byte(`{"item":"widget","delta":-1}`)},
		{ID: durable.ID{Producer: producer, Sequence: 3}, Codec: inventory.Codec,
			Payload: []byte(`{"item":"widget","delta":2}`)},
	}
}

func effectRequest(delivery durable.Delivery) inventory.Request {
	return inventory.Request{
		Key: inventory.Key{Namespace: sourceNamespace, Producer: delivery.ID.Producer,
			Sequence: delivery.ID.Sequence, Effect: "inventory.adjust"},
		Codec: delivery.Codec, Payload: delivery.Payload,
	}
}

func sourceIDs() []durable.ID {
	entries := sourceEntries()
	ids := make([]durable.ID, len(entries))
	for index, entry := range entries {
		ids[index] = entry.ID
	}
	return ids
}

func exampleConfig() (durable.Config, error) {
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 1024, MaxCodec: 64,
		MaxPendingItems: 16, MaxPendingPayload: 16384,
		MaxReceipts: 64, MaxProducers: 4, DatabaseLimit: 64 << 20,
	}
	runtime, err := durable.DefaultRuntime(policy)
	if err != nil {
		return durable.Config{}, err
	}
	return durable.Config{Policy: policy, Durability: durable.Synchronized, Runtime: runtime}, nil
}
