//go:build durable_faulttest && linux && amd64 && cgo

package sqlitefault

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImageOracle(t *testing.T) {
	if scenario := os.Getenv("WORKQ_IMAGE_SCENARIO"); scenario != "" {
		runImageChild(t, scenario)
		return
	}
	for _, tc := range []struct {
		name, main, wal string
		directory       bool
	}{
		{"unsynced-write", "base-long", "wal-base", true},
		{"unsynced-truncate", "base-long", "wal-base", true},
		{"independent-sync", "new-main-longer", "wal-base", true},
		{"synced-truncate", "bas", "wal-base", true},
		{"create-lost", "base-long", "", true},
		{"create-committed", "base-long", "created", true},
		{"delete-rollback", "base-long", "wal-base", true},
		{"delete-committed", "", "wal-base", true},
		{"parent-only", "", "", true},
		{"native-only", "", "", true},
		{"child-only", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := crashImageChild(t, tc.name)
			checkImageLengths(t, manifest)
			if tc.name == "independent-sync" || tc.name == "unsynced-write" {
				assertVisibleImage(t, manifest, "queue.sqlite3", "new-main-longer")
				assertVisibleImage(t, manifest, "queue.sqlite3-wal", "new-wal-longer")
			}
			if tc.name == "unsynced-truncate" {
				assertVisibleImage(t, manifest, "queue.sqlite3", "bas")
			}
			mapping, err := Restore(manifest)
			require.NoError(t, err)
			if !tc.directory {
				_, err := os.Stat(manifest.Root)
				assert.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			assert.NotEmpty(t, mapping)
			for _, expected := range []struct{ name, contents string }{
				{"queue.sqlite3", tc.main}, {"queue.sqlite3-wal", tc.wal},
			} {
				data, err := os.ReadFile(filepath.Join(manifest.Root, expected.name))
				if expected.contents == "" {
					assert.ErrorIs(t, err, os.ErrNotExist)
					continue
				}
				require.NoError(t, err)
				assert.Equal(t, expected.contents, string(data))
			}
			_, err = os.Stat(filepath.Join(manifest.Root, "queue.sqlite3-shm"))
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

// Successful fixtures are removed. Failed fixtures retain their images and manifest for diagnosis.
func imageFixture(t *testing.T) (string, string) {
	t.Helper()
	parent, err := os.MkdirTemp("", "image-fixture-")
	require.NoError(t, err)
	scratch, err := os.MkdirTemp("", "image-evidence-")
	require.NoError(t, err)
	root := filepath.Join(parent, "queue")
	require.NoError(t, os.Mkdir(root, 0700))
	t.Logf("fixture %s evidence %s", root, scratch)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		assert.NoError(t, os.RemoveAll(parent))
		assert.NoError(t, os.RemoveAll(scratch))
	})
	return root, scratch
}

func crashImageChild(t *testing.T, scenario string) Manifest {
	t.Helper()
	root, scratch := imageFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestImageChild$", "-test.v")
	command.Env = append(os.Environ(), "WORKQ_IMAGE_SCENARIO="+scenario,
		"WORKQ_IMAGE_ROOT="+root, "WORKQ_IMAGE_SCRATCH="+scratch)
	command.ExtraFiles = []*os.File{writer}
	command.Stderr = os.Stderr
	command.Stdout = os.Stderr
	require.NoError(t, command.Start())
	require.NoError(t, writer.Close())
	joined := false
	t.Cleanup(func() {
		if joined {
			return
		}
		_ = command.Process.Kill() // Cleanup must reap even when an assertion fails.
		_ = command.Wait()
	})
	var manifest Manifest
	require.NoError(t, json.NewDecoder(reader).Decode(&manifest))
	require.NoError(t, command.Process.Kill())
	err = command.Wait()
	joined = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, syscall.SIGKILL, exit.Sys().(syscall.WaitStatus).Signal())
	require.Len(t, manifest.Events, int(manifest.Sequence))
	return manifest
}

func TestImageChild(t *testing.T) {
	scenario := os.Getenv("WORKQ_IMAGE_SCENARIO")
	if scenario == "" {
		t.Skip("child process entry")
	}
	runImageChild(t, scenario)
}

func imageSync(t *testing.T, controller *Controller, path string) {
	t.Helper()
	id, err := pathIdentity(path)
	require.NoError(t, err)
	file := controller.model.pins[id]
	if file == nil {
		file, err = os.Open(path)
		require.NoError(t, err)
		require.NoError(t, controller.model.retain(file))
	}
	after, err := controller.ObserveOwned(file, path)
	require.NoError(t, err)
	require.NoError(t, after(file.Sync()))
}

func runImageChild(t *testing.T, scenario string) {
	root := os.Getenv("WORKQ_IMAGE_ROOT")
	scratch := os.Getenv("WORKQ_IMAGE_SCRATCH")
	name, controller, err := Attach(root, Limits{Events: 8192, Scratch: scratch})
	require.NoError(t, err)
	lock, err := os.OpenFile(filepath.Join(root, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	defer func() { assert.NoError(t, lock.Close()) }() // Keep ownership until process death.
	main := filepath.Join(root, "queue.sqlite3")
	wal := main + "-wal"
	if scenario == "FULL" || scenario == "NORMAL" || scenario == "native-only" {
		rawImageWorkload(t, controller, name, scenario, main)
	} else {
		require.NoError(t, os.WriteFile(main, []byte("base-long"), 0600))
		imageSync(t, controller, main)
		if scenario != "create-lost" && scenario != "create-committed" {
			require.NoError(t, os.WriteFile(wal, []byte("wal-base"), 0600))
			imageSync(t, controller, wal)
		}
		if scenario != "parent-only" {
			imageSync(t, controller, root)
		}
		if scenario != "child-only" {
			imageSync(t, controller, filepath.Dir(root))
		}
		switch scenario {
		case "unsynced-write", "independent-sync":
			require.NoError(t, os.WriteFile(main, []byte("new-main-longer"), 0600))
			require.NoError(t, os.WriteFile(wal, []byte("new-wal-longer"), 0600))
			if scenario == "independent-sync" {
				imageSync(t, controller, main)
			}
		case "unsynced-truncate", "synced-truncate":
			require.NoError(t, os.Truncate(main, 3))
			if scenario == "synced-truncate" {
				imageSync(t, controller, main)
			}
		case "create-lost", "create-committed":
			require.NoError(t, os.WriteFile(wal, []byte("created"), 0600))
			imageSync(t, controller, wal)
			if scenario == "create-committed" {
				imageSync(t, controller, root)
			}
		case "delete-rollback", "delete-committed":
			require.NoError(t, os.Remove(main))
			if scenario == "delete-committed" {
				imageSync(t, controller, root)
			} else {
				require.NoError(t, os.WriteFile(main, []byte("replacement"), 0600))
				imageSync(t, controller, main)
			}
		}
	}
	manifest, err := controller.Freeze()
	require.NoError(t, err)
	pipe := os.NewFile(3, "freeze")
	require.NoError(t, json.NewEncoder(pipe).Encode(manifest))
	// Keep all SQLite handles and the permanent lock live until the parent kills us.
	for {
		_ = syscall.Pause()
	} // Parent owns termination.
}

func rawImageWorkload(t *testing.T, controller *Controller, name, policy, main string) {
	location := url.URL{Scheme: "file", Path: main}
	location.RawQuery = url.Values{"vfs": {name}, "_synchronous": {"FULL"}}.Encode()
	db, err := sql.Open("sqlite3", location.String())
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
		"CREATE TABLE records(group_id INTEGER, value INTEGER)",
		"PRAGMA wal_checkpoint(TRUNCATE)",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
	if policy == "native-only" {
		imageSync(t, controller, filepath.Dir(filepath.Dir(main)))
		return
	}
	imageSync(t, controller, main)
	imageSync(t, controller, main+"-wal")
	imageSync(t, controller, filepath.Dir(main))
	imageSync(t, controller, filepath.Dir(filepath.Dir(main)))
	_, err = db.ExecContext(t.Context(), "PRAGMA synchronous="+policy)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(),
		"BEGIN IMMEDIATE; INSERT INTO records VALUES(1,11),(1,22); COMMIT; "+
			"BEGIN IMMEDIATE; INSERT INTO records VALUES(2,33),(2,44); COMMIT")
	require.NoError(t, err)
}

func TestRawSQLiteImages(t *testing.T) {
	for _, policy := range []string{"FULL", "NORMAL"} {
		t.Run(policy, func(t *testing.T) {
			manifest := crashImageChild(t, policy)
			_, err := Restore(manifest)
			require.NoError(t, err)
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestImageRecovery$")
			command.Env = append(os.Environ(), "WORKQ_IMAGE_RECOVER="+manifest.Root,
				"WORKQ_IMAGE_POLICY="+policy)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func TestImageRecovery(t *testing.T) {
	root := os.Getenv("WORKQ_IMAGE_RECOVER")
	if root == "" {
		t.Skip("recovery child entry")
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "queue.sqlite3"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	rows, err := db.Query("SELECT group_id,value FROM records ORDER BY group_id,value")
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	type record struct {
		groupID int
		value   int
	}
	var recovered []record
	for rows.Next() {
		var item record
		require.NoError(t, rows.Scan(&item.groupID, &item.value))
		recovered = append(recovered, item)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	if os.Getenv("WORKQ_IMAGE_POLICY") == "FULL" {
		assert.Equal(t, []record{{1, 11}, {1, 22}, {2, 33}, {2, 44}}, recovered)
	} else {
		assert.Empty(t, recovered, "NORMAL group was visible but unsynchronized")
	}
	var integrity string
	require.NoError(t, db.QueryRow("PRAGMA integrity_check").Scan(&integrity))
	assert.Equal(t, "ok", integrity)
}

func TestImageBounds(t *testing.T) {
	t.Run("image", func(t *testing.T) {
		root, scratch := imageFixture(t)
		_, controller, err := Attach(root, Limits{Events: 8192, Scratch: scratch})
		require.NoError(t, err)
		path := filepath.Join(root, "queue.sqlite3")
		file, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, file.Truncate(imageBudget+1))
		after, err := controller.ObserveOwned(file, path)
		require.NoError(t, err)
		require.ErrorIs(t, after(file.Sync()), ErrObservation)
		require.NoError(t, file.Close())
		require.ErrorIs(t, controller.Close(), ErrObservation)
	})
	t.Run("identity", func(t *testing.T) {
		model := &imageModel{identities: make(map[Identity]struct{})}
		for i := range identityBudget {
			require.NoError(t, model.register(Identity{Inode: uint64(i + 1)}))
		}
		assert.ErrorIs(t, model.register(Identity{Inode: 99}), ErrObservation)
	})
	t.Run("event", func(t *testing.T) {
		controller := &Controller{limits: Limits{Events: 8192}}
		for range 8192 {
			require.NoError(t, controller.After(Event{}, nil))
		}
		assert.ErrorIs(t, controller.After(Event{}, nil), ErrObservation)
	})
	t.Run("live-restore", func(t *testing.T) {
		_, err := Restore(Manifest{PID: os.Getpid()})
		assert.True(t, errors.Is(err, ErrObservation))
	})
}

func TestFreezeCompletedBoundary(t *testing.T) {
	root, scratch := imageFixture(t)
	_, controller, err := Attach(root, Limits{Events: 8192, Scratch: scratch})
	require.NoError(t, err)
	file, err := os.Create(filepath.Join(root, "queue.sqlite3"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, file.Close()) }()
	_, err = file.WriteString("completed-boundary")
	require.NoError(t, err)
	after, err := controller.ObserveOwned(file, file.Name())
	require.NoError(t, err)
	_, err = controller.Freeze()
	assert.ErrorIs(t, err, ErrObservation, "Before is not a completed synchronization boundary")
	require.NoError(t, after(file.Sync()))
	manifest, err := controller.Freeze()
	require.NoError(t, err)
	require.Len(t, manifest.Images, 1)
	before, err := os.ReadFile(manifest.Images[0].Synchronized)
	require.NoError(t, err)
	_, err = controller.ObserveOwned(file, file.Name())
	assert.ErrorIs(t, err, ErrObservation, "a frozen oracle refuses further callbacks")
	current, err := os.ReadFile(manifest.Images[0].Synchronized)
	require.NoError(t, err)
	assert.Equal(t, before, current)
	assert.ErrorIs(t, controller.Close(), ErrObservation)
}

func checkImageLengths(t *testing.T, manifest Manifest) {
	t.Helper()
	for _, image := range manifest.Images {
		for _, version := range []struct {
			path   string
			length int64
		}{
			{image.Visible, image.VisibleLength}, {image.Synchronized, image.SynchronizedLength},
		} {
			data, err := os.ReadFile(version.path)
			require.NoError(t, err)
			assert.Equal(t, version.length, int64(len(data)))
		}
	}
	encoded, err := json.Marshal(manifest.Events)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "base-long")
	assert.NotContains(t, string(encoded), "new-main-longer")
}

func assertVisibleImage(t *testing.T, manifest Manifest, name, expected string) {
	t.Helper()
	id := manifest.Visible[name]
	for _, image := range manifest.Images {
		if image.Identity != id {
			continue
		}
		data, err := os.ReadFile(image.Visible)
		require.NoError(t, err)
		assert.Equal(t, expected, string(data))
		return
	}
	t.Fatalf("missing visible image for %s", name)
}
