//go:build durable_faulttest && linux && amd64 && cgo

package sqlitefault

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const imageBudget = 64 << 20
const identityBudget = 8

// Image records lengths independently from its bounded scratch files.
type Image struct {
	Identity           Identity `json:"identity"`
	Role               string   `json:"role"`
	Visible            string   `json:"visible"`
	Synchronized       string   `json:"synchronized"`
	VisibleLength      int64    `json:"visible_length"`
	SynchronizedLength int64    `json:"synchronized_length"`
}

// Manifest describes a completed observation boundary, never a hardware power-loss claim.
// Restore requires the recording process to have died and been reaped first.
type Manifest struct {
	PID              int                 `json:"pid"`
	Root             string              `json:"root"`
	Scratch          string              `json:"scratch"`
	Sequence         uint64              `json:"sequence"`
	Directory        Identity            `json:"directory"`
	Parent           Identity            `json:"parent"`
	DirectoryDurable bool                `json:"directory_durable"`
	Visible          map[string]Identity `json:"visible"`
	Synchronized     map[string]Identity `json:"synchronized"`
	Images           []Image             `json:"images"`
	Events           []Event             `json:"events"`
}

type imageModel struct {
	manifest   Manifest
	identities map[Identity]struct{}
	pins       map[Identity]*os.File
	frozen     bool
}

func newImageModel(root, scratch string) (*imageModel, error) {
	scratch, err := filepath.EvalSymlinks(scratch)
	if err != nil {
		return nil, err
	}
	scratch, err = filepath.Abs(scratch)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(filepath.Dir(root), scratch)
	if err != nil {
		return nil, err
	}
	if filepath.IsLocal(relative) || relative == "." {
		return nil, fmt.Errorf("scratch overlaps fixture parent: %w", ErrObservation)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, fmt.Errorf("scratch is not empty: %w", ErrObservation)
	}
	model := &imageModel{
		manifest: Manifest{PID: os.Getpid(), Root: root, Scratch: scratch,
			Visible: make(map[string]Identity), Synchronized: make(map[string]Identity)},
		identities: make(map[Identity]struct{}),
		pins:       make(map[Identity]*os.File),
	}
	model.manifest.Directory, err = pathIdentity(root)
	if err != nil {
		return nil, err
	}
	model.manifest.Parent, err = pathIdentity(filepath.Dir(root))
	if err != nil {
		return nil, err
	}
	for _, id := range []Identity{model.manifest.Directory, model.manifest.Parent} {
		if err := model.register(id); err != nil {
			return nil, err
		}
	}
	for _, path := range []string{root, filepath.Dir(root)} {
		file, err := os.Open(path)
		if err != nil {
			return nil, errors.Join(err, model.close())
		}
		if err := model.retain(file); err != nil {
			return nil, errors.Join(err, model.close())
		}
	}
	if err := model.scan(); err != nil {
		return nil, errors.Join(err, model.close())
	}
	return model, nil
}

func pathIdentity(path string) (Identity, error) {
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err != nil {
		return Identity{}, err
	}
	return identity(&stat), nil
}

func (m *imageModel) register(id Identity) error {
	if _, ok := m.identities[id]; ok {
		return nil
	}
	if len(m.identities) == identityBudget {
		return fmt.Errorf("identity cap: %w", ErrObservation)
	}
	m.identities[id] = struct{}{}
	return nil
}

var artifactRoles = map[string]string{
	"queue.sqlite3": "main", "queue.sqlite3-wal": "wal", "LOCK": "lock",
	"queue.sqlite3-shm": "shm", "queue.sqlite3-journal": "journal",
}

func (m *imageModel) scan() error {
	visible := make(map[string]Identity)
	for name, role := range artifactRoles {
		path := filepath.Join(m.manifest.Root, name)
		id, err := pathIdentity(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := m.register(id); err != nil {
			return err
		}
		visible[name] = id
		file := m.pins[id]
		if file == nil {
			file, err = os.Open(path)
			if err != nil {
				return err
			}
			if err := m.retain(file); err != nil {
				return err
			}
		}
		if role != "main" && role != "wal" {
			continue
		}
		if err := m.capture(file.Fd(), id, role, false); err != nil {
			return err
		}

	}
	m.manifest.Visible = visible
	return nil
}

// Pins prevent kernel inode reuse and stay open until SQLite has released its POSIX locks.
func (m *imageModel) retain(file *os.File) error {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &stat); err != nil {
		return errors.Join(err, file.Close())
	}
	id := identity(&stat)
	if err := m.register(id); err != nil {
		return errors.Join(err, file.Close())
	}
	m.pins[id] = file
	return nil
}

func (m *imageModel) close() error {
	var result error
	for _, file := range m.pins {
		result = errors.Join(result, file.Close())
	}
	m.pins = nil
	return result
}

func (m *imageModel) capture(fd uintptr, id Identity, role string, promote bool) error {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(fd), &stat); err != nil {
		return err
	}
	if identity(&stat) != id {
		return ErrObservation
	}
	if err := m.register(id); err != nil {
		return err
	}
	if stat.Size < 0 || stat.Size > imageBudget {
		return ErrObservation
	}
	if m.pins[id] == nil {
		duplicate, err := syscall.Dup(int(fd))
		if err != nil {
			return err
		}
		syscall.CloseOnExec(duplicate)
		file := os.NewFile(uintptr(duplicate), "oracle-pin")
		if err := m.retain(file); err != nil {
			return err
		}
	}
	index := -1
	for i := range m.manifest.Images {
		if m.manifest.Images[i].Identity == id {
			index = i
			break
		}
	}
	if index < 0 {
		index = len(m.manifest.Images)
		prefix := filepath.Join(m.manifest.Scratch, fmt.Sprintf("image-%d", index))
		m.manifest.Images = append(m.manifest.Images, Image{
			Identity: id, Role: role, Visible: prefix + "-visible", Synchronized: prefix + "-sync",
		})
		if err := os.WriteFile(prefix+"-sync", nil, 0600); err != nil {
			return err
		}
	}
	image := &m.manifest.Images[index]
	var retained int64
	for _, current := range m.manifest.Images {
		retained += current.VisibleLength + current.SynchronizedLength
	}
	retained += stat.Size - image.VisibleLength
	if promote {
		retained += stat.Size - image.SynchronizedLength
	}
	if stat.Size < 0 || retained > imageBudget {
		return fmt.Errorf("image cap: %w", ErrObservation)
	}
	if err := copyDescriptor(fd, image.Visible, stat.Size); err != nil {
		return err
	}
	image.VisibleLength = stat.Size
	if promote {
		if err := copyDescriptor(fd, image.Synchronized, stat.Size); err != nil {
			return err
		}
		image.SynchronizedLength = stat.Size
	}
	return nil
}

// copyDescriptor uses pread because the borrowed fd must never acquire an os.File finalizer.
func copyDescriptor(fd uintptr, destination string, length int64) error {
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	var buffer [32 << 10]byte
	var offset int64
	for offset < length {
		amount := min(int64(len(buffer)), length-offset)
		count, readErr := syscall.Pread(int(fd), buffer[:amount], offset)
		if readErr != nil {
			return errors.Join(readErr, file.Close())
		}
		if count == 0 {
			return errors.Join(io.ErrUnexpectedEOF, file.Close())
		}
		if _, err := file.Write(buffer[:count]); err != nil {
			return errors.Join(err, file.Close())
		}
		offset += int64(count)
	}
	return errors.Join(file.Sync(), file.Close())
}

func (m *imageModel) observe(event Event) error {
	if event.Actual != event.Expected {
		return ErrObservation
	}
	if event.Operation == "native-close" || event.Operation == "native-delete" {
		return m.scan()
	}
	if event.Role == "main" || event.Role == "wal" {
		promote := event.Performed && event.ActualResult == 0 &&
			(event.Operation == "native-sync" || event.Operation == "owned-sync")
		if err := m.capture(event.Descriptor, event.Actual, event.Role, promote); err != nil {
			return err
		}
	}
	if err := m.scan(); err != nil {
		return err
	}
	if event.Operation != "owned-sync" || !event.Performed || event.ActualResult != 0 {
		return nil
	}
	switch event.Role {
	case "directory":
		if event.Actual != m.manifest.Directory {
			return ErrObservation
		}
		m.manifest.Synchronized = make(map[string]Identity)
		for name, id := range m.manifest.Visible {
			if artifactRoles[name] == "shm" || artifactRoles[name] == "journal" {
				continue
			}
			m.manifest.Synchronized[name] = id
		}
	case "parent":
		if event.Actual != m.manifest.Parent {
			return ErrObservation
		}
		current, err := pathIdentity(m.manifest.Root)
		if err != nil {
			return err
		}
		m.manifest.DirectoryDurable = current == m.manifest.Directory
	}
	return nil
}

// Freeze writes a bounded completed-boundary manifest outside the modeled namespace.
// The caller must park the child immediately, without closing the queue or SQLite connection.
func (c *Controller) Freeze() (Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return Manifest{}, c.failure
	}
	if c.model == nil || c.model.frozen || c.pending != 0 {
		return Manifest{}, ErrObservation
	}
	if err := c.model.scan(); err != nil {
		return Manifest{}, c.failObservation(err)
	}
	manifest := c.model.manifest
	manifest.Sequence = c.sequence
	manifest.Events = append([]Event(nil), c.events...)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, c.failObservation(err)
	}
	manifestPath := filepath.Join(manifest.Scratch, "manifest.json")
	if err := os.WriteFile(manifestPath, encoded, 0600); err != nil {
		return Manifest{}, c.failObservation(err)
	}
	c.model.frozen = true
	return manifest, nil
}

// Restore replaces only the isolated fixture after its child has died and been reaped.
// The returned mapping associates logical pre-crash artifacts with newly allocated OS identities.
func Restore(manifest Manifest) (map[Identity]Identity, error) {
	if manifest.PID <= 0 || !errors.Is(syscall.Kill(manifest.PID, 0), syscall.ESRCH) {
		return nil, fmt.Errorf("recording child still exists: %w", ErrObservation)
	}
	lock, err := os.OpenFile(filepath.Join(manifest.Root, "LOCK"), os.O_RDWR, 0)
	if err == nil {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return nil, errors.Join(err, lock.Close())
		}
		if err := lock.Close(); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	entries, err := os.ReadDir(manifest.Root)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if _, ok := artifactRoles[entry.Name()]; !ok || entry.IsDir() {
			return nil, fmt.Errorf("unexpected fixture entry: %w", ErrObservation)
		}
	}
	for _, entry := range entries {
		if err := os.Remove(filepath.Join(manifest.Root, entry.Name())); err != nil {
			return nil, err
		}
	}
	if err := os.Remove(manifest.Root); err != nil {
		return nil, err
	}
	mapping := make(map[Identity]Identity)
	if !manifest.DirectoryDurable {
		return mapping, recordRestoredIdentities(manifest.Scratch, mapping)
	}
	if err := os.Mkdir(manifest.Root, 0700); err != nil {
		return nil, err
	}
	directory, err := pathIdentity(manifest.Root)
	if err != nil {
		return nil, err
	}
	mapping[manifest.Directory] = directory
	for name, id := range manifest.Synchronized {
		destination := filepath.Join(manifest.Root, name)
		if name == "LOCK" {
			if err := os.WriteFile(destination, nil, 0600); err != nil {
				return nil, err
			}
		} else {
			found := false
			for _, image := range manifest.Images {
				if image.Identity != id {
					continue
				}
				found = true
				source, err := os.Open(image.Synchronized)
				if err != nil {
					return nil, err
				}
				err = copyDescriptor(source.Fd(), destination, image.SynchronizedLength)
				if err := errors.Join(err, source.Close()); err != nil {
					return nil, err
				}
				break
			}
			if !found {
				return nil, ErrObservation
			}
		}
		actual, err := pathIdentity(destination)
		if err != nil {
			return nil, err
		}
		mapping[id] = actual
	}
	return mapping, recordRestoredIdentities(manifest.Scratch, mapping)
}

func recordRestoredIdentities(scratch string, mapping map[Identity]Identity) error {
	type restoredIdentity struct {
		Logical Identity `json:"logical"`
		Actual  Identity `json:"actual"`
	}
	records := make([]restoredIdentity, 0, len(mapping))
	for logical, actual := range mapping {
		records = append(records, restoredIdentity{Logical: logical, Actual: actual})
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(scratch, "restored-identities.json"), encoded, 0600)
}
