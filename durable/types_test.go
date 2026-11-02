package durable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDurabilityString(t *testing.T) {
	t.Parallel()
	var zero Durability
	tests := []struct {
		name       string
		durability Durability
		want       string
	}{
		{name: "zero", durability: zero, want: "synchronized"},
		{name: "synchronized", durability: Synchronized, want: "synchronized"},
		{name: "process-crash", durability: ProcessCrash, want: "process-crash"},
		{name: "unknown255", durability: Durability(255), want: "Durability(255)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.durability.String())
		})
	}
}
