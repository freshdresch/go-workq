//go:build !cgo

package inventory

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenWithoutCGOPreservesPaths(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "absent"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "inventory.db")
			original := []byte("operator-owned database contents")
			if existing {
				require.NoError(t, os.WriteFile(path, original, 0600))
			}
			store, err := Open(t.Context(), path, 2)
			assert.Error(t, err)
			assert.Nil(t, store)
			if existing {
				contents, err := os.ReadFile(path)
				require.NoError(t, err)
				assert.Equal(t, original, contents)
				return
			}
			_, err = os.Stat(path)
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
