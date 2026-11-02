//go:build durable_faulttest && linux && amd64 && cgo

// Package sqlitefault provides the pinned, test-only SQLite VFS and sync observer.
// Its optional disk oracle models loss of unsynchronized bytes and namespace entries.
package sqlitefault

/*
#cgo CFLAGS: -Wall -Wextra -Werror
#include <stdlib.h>
#include "abi.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/cgo"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	// Link the pinned bundled SQLite symbols used by the C forwarding shim.
	_ "github.com/mattn/go-sqlite3"
)

// Identity identifies an actual filesystem object.
type Identity struct{ Device, Inode uint64 }

// Event records metadata only. Descriptor is borrowed for the synchronous callback.
type Event struct {
	Sequence       uint64
	Operation      string
	Phase          string
	Role           string
	Actual         Identity
	Expected       Identity
	Parent         Identity
	Name           string
	Descriptor     uintptr
	Offset, Length int64
	Result         int
	Errno          syscall.Errno
}

// Limits bounds event retention. Overflow fails observation and the storage operation.
type Limits struct {
	Events int
	// Scratch enables bounded disk images in an existing, isolated directory outside root.
	Scratch string
}

// Effect selects the one supported round 8a fault.
type Effect uint8

const (
	// SyncError returns SQLITE_IOERR_FSYNC before a native or owned sync.
	SyncError Effect = iota + 1
)

// ErrObservation means the harness could not establish its observation.
var ErrObservation = errors.New("sqlite fault observation rejected")

// ErrInjected identifies a deliberately refused sync.
var ErrInjected = errors.New("injected sync failure")

type armedFault struct {
	operation, role, phase string
	remaining              uint64
}

// Controller owns one fixture. Close it only after every connection and callback ends.
// Fixtures must not overlap or run in parallel within a child process.
type Controller struct {
	root, name   string
	limits       Limits
	registration unsafe.Pointer
	handle       cgo.Handle
	mu           sync.Mutex
	events       []Event
	sequence     uint64
	pending      int
	fault        *armedFault
	failure      error
	model        *imageModel
}

var nextName atomic.Uint64

// Attach registers a uniquely named VFS for exactly root's queue files.
// The caller must close all connections before calling Controller.Close.
func Attach(root string, limits Limits) (string, *Controller, error) {
	if limits.Events <= 0 || limits.Events > 8192 {
		return "", nil, ErrObservation
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", nil, err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", nil, err
	}
	controller := &Controller{
		root:   canonical,
		name:   fmt.Sprintf("workq-fault-%d", nextName.Add(1)),
		limits: limits,
		events: make([]Event, 0, limits.Events),
	}
	if limits.Scratch != "" {
		model, err := newImageModel(canonical, limits.Scratch)
		if err != nil {
			return "", nil, err
		}
		controller.model = model
	}
	controller.handle = cgo.NewHandle(controller)
	name := C.CString(controller.name)
	defer C.free(unsafe.Pointer(name))
	controller.registration = C.fault_register(name, C.uintptr_t(controller.handle))
	if controller.registration == nil {
		controller.handle.Delete()
		if controller.model != nil {
			if err := controller.model.close(); err != nil {
				return "", nil, err
			}
		}
		return "", nil, fmt.Errorf("unsupported pinned unix SQLite ABI: %w", ErrObservation)
	}
	return controller.name, controller, nil
}

// Close unregisters the VFS. A live native file prevents unregistering it.
func (c *Controller) Close() error {
	if c.registration == nil {
		return ErrObservation
	}
	if result := C.fault_unregister(c.registration); result != 0 {
		return fmt.Errorf("unregister VFS (%d): %w", result, ErrObservation)
	}
	c.registration = nil
	c.handle.Delete()
	var closeErr error
	if c.model != nil {
		closeErr = c.model.close()
	}
	return errors.Join(c.Err(), closeErr)
}

// VFS returns the fixture's VFS only for its main database path.
func (c *Controller) VFS(path string) string {
	if path == filepath.Join(c.root, "queue.sqlite3") {
		return c.name
	}
	return ""
}

func (c *Controller) role(path string) (string, string, error) {
	if path == c.root {
		return "directory", ".", nil
	}
	if path == filepath.Dir(c.root) {
		return "parent", "..", nil
	}
	switch path {
	case filepath.Join(c.root, "queue.sqlite3"):
		return "main", "queue.sqlite3", nil
	case filepath.Join(c.root, "queue.sqlite3-wal"):
		return "wal", "queue.sqlite3-wal", nil
	case filepath.Join(c.root, "queue.sqlite3-journal"):
		return "journal", "queue.sqlite3-journal", nil
	case filepath.Join(c.root, "queue.sqlite3-shm"):
		return "shm", "queue.sqlite3-shm", nil
	case filepath.Join(c.root, "LOCK"):
		return "lock", "LOCK", nil
	}
	return "", "", ErrObservation
}

func identity(stat *syscall.Stat_t) Identity {
	return Identity{Device: uint64(stat.Dev), Inode: stat.Ino}
}

// DescriptorEvent reads fstat on the borrowed descriptor and lstat on its expected name.
// A mismatch is retained as evidence but cannot become a successful sync.
func (c *Controller) DescriptorEvent(path string, descriptor uintptr) (Event, error) {
	role, name, err := c.role(path)
	if err != nil {
		return Event{}, err
	}
	event := Event{Role: role, Name: name, Descriptor: descriptor}
	var actual, expected, parent syscall.Stat_t
	if err := syscall.Fstat(int(descriptor), &actual); err != nil {
		return event, err
	}
	event.Actual = identity(&actual)
	if err := syscall.Lstat(path, &expected); err != nil {
		return event, err
	}
	event.Expected = identity(&expected)
	if err := syscall.Stat(filepath.Dir(path), &parent); err != nil {
		return event, err
	}
	event.Parent = identity(&parent)
	return event, nil
}

// The caller holds mu. Keep the first failure so repeated callbacks stay bounded.
func (c *Controller) failObservation(err error) error {
	if c.failure == nil {
		c.failure = errors.Join(ErrObservation, err)
	}
	return c.failure
}

func (c *Controller) observeDescriptor(path string, descriptor uintptr) (Event, error) {
	event, err := c.DescriptorEvent(path, descriptor)
	if err == nil {
		return event, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return event, c.failObservation(err)
}

func (c *Controller) append(event Event) error {
	if c.failure != nil {
		return c.failure
	}
	if c.model != nil && c.model.frozen {
		return c.failObservation(ErrObservation)
	}
	if len(c.events) == c.limits.Events {
		c.failure = fmt.Errorf("event limit: %w", ErrObservation)
		return c.failure
	}
	c.sequence++
	event.Sequence = c.sequence
	c.events = append(c.events, event)
	return nil
}

// Before records an attempted operation and consumes a matching armed fault once.
func (c *Controller) Before(event Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	event.Phase = "before"
	if err := c.append(event); err != nil {
		return err
	}
	if event.Actual != event.Expected {
		return c.failObservation(ErrObservation)
	}
	if c.model != nil {
		c.pending++
	}
	if c.fault == nil {
		return nil
	}
	fault := c.fault
	matches := fault.operation == event.Operation && fault.role == event.Role &&
		fault.phase == event.Phase
	if !matches {
		return nil
	}
	fault.remaining--
	if fault.remaining != 0 {
		return nil
	}
	c.fault = nil
	return ErrInjected
}

// After records the actual outcome. A mismatch or failed observation never records success.
func (c *Controller) After(event Event, result error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	event.Phase = "after"
	var errno syscall.Errno
	if errors.As(result, &errno) {
		event.Errno = errno
	}
	if result != nil && event.Result == 0 {
		event.Result = 1034
	}
	if event.Actual != event.Expected {
		event.Result = 1034
		result = errors.Join(result, ErrObservation)
	}
	appendErr := c.append(event)
	if appendErr == nil && c.model != nil {
		if c.pending == 0 {
			return c.failObservation(ErrObservation)
		}
		c.pending--
		if err := c.model.observe(event); err != nil {
			appendErr = c.failObservation(err)
		}
	}
	if errors.Is(result, ErrObservation) {
		appendErr = errors.Join(appendErr, c.failObservation(result))
	}
	return errors.Join(result, appendErr)
}

// Arm selects the occurrence among matching Before events after this call.
// Round 8a supports only sync errors before native-sync or owned-sync.
func (c *Controller) Arm(
	operation, role string,
	occurrence uint64,
	phase string,
	effect Effect,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fault != nil || occurrence == 0 || phase != "before" || effect != SyncError {
		return ErrObservation
	}
	if operation != "native-sync" && operation != "owned-sync" {
		return ErrObservation
	}
	c.fault = &armedFault{operation: operation, role: role, phase: phase, remaining: occurrence}
	return nil
}

// Events returns an owned copy of the bounded metadata log.
func (c *Controller) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}

// Err reports a sticky observer failure such as event overflow.
func (c *Controller) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

// ObserveOwned brackets the production helper's actual Sync call.
// The returned function must run even if Before injects an error.
func (c *Controller) ObserveOwned(file *os.File, path string) (func(error) error, error) {
	event, err := c.observeDescriptor(path, file.Fd())
	if err != nil {
		return func(result error) error { return errors.Join(result, err) }, err
	}
	event.Operation = "owned-sync"
	return func(result error) error {
		after, err := c.observeDescriptor(path, file.Fd())
		if err != nil {
			return errors.Join(result, err, ErrObservation)
		}
		after.Operation = event.Operation
		return c.After(after, result)
	}, c.Before(event)
}

//export goFaultPath
func goFaultPath(handle C.uintptr_t, path *C.char) C.int {
	controller := cgo.Handle(handle).Value().(*Controller)
	role, _, err := controller.role(C.GoString(path))
	if err != nil || (role != "main" && role != "wal" && role != "journal") {
		return 14
	}
	return 0
}

//export goFaultEvent
func goFaultEvent(
	handle C.uintptr_t, path *C.char, descriptor C.int,
	operation C.int, phase C.int, result C.int, errno C.int,
	offset C.longlong, length C.longlong,
) C.int {
	controller := cgo.Handle(handle).Value().(*Controller)
	event, err := controller.observeDescriptor(C.GoString(path), uintptr(descriptor))
	if err != nil {
		return 1034
	}
	event.Operation = map[int]string{1: "native-open", 2: "native-sync",
		3: "native-write", 4: "native-truncate"}[int(operation)]
	event.Offset, event.Length = int64(offset), int64(length)
	event.Result = int(result)
	event.Errno = syscall.Errno(errno)
	if phase == 1 {
		err = controller.Before(event)
	} else {
		var outcome error
		if result != 0 {
			outcome = ErrInjected
		}
		err = controller.After(event, outcome)
	}
	if err != nil {
		return 1034
	}
	return 0
}

//export goFaultDelete
func goFaultDelete(
	handle C.uintptr_t, path *C.char,
	device C.ulonglong, inode C.ulonglong,
	phase C.int, result C.int, errno C.int,
) C.int {
	controller := cgo.Handle(handle).Value().(*Controller)
	role, name, err := controller.role(C.GoString(path))
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if err != nil {
		_ = controller.failObservation(err) // Retained here and returned as SQLite failure below.
		return 1034
	}
	parent, err := pathIdentity(controller.root)
	if err != nil {
		_ = controller.failObservation(err) // Retained here and returned as SQLite failure below.
		return 1034
	}
	id := Identity{Device: uint64(device), Inode: uint64(inode)}
	event := Event{Operation: "native-delete", Phase: "after", Role: role, Name: name,
		Actual: id, Expected: id, Parent: parent, Result: int(result), Errno: syscall.Errno(errno)}
	if phase == 1 {
		event.Phase = "before"
	}
	if err := controller.append(event); err != nil {
		return 1034
	}
	if controller.model != nil {
		if phase == 1 {
			controller.pending++
			return 0
		}
		if controller.pending == 0 {
			return 1034
		}
		controller.pending--
		if err := controller.model.scan(); err != nil {
			_ = controller.failObservation(err) // Retained here and returned as SQLite failure below.
			return 1034
		}
	}
	return 0
}

//export goFaultOpenBoundary
func goFaultOpenBoundary(handle C.uintptr_t, path *C.char, phase C.int, result C.int) C.int {
	controller := cgo.Handle(handle).Value().(*Controller)
	controller.mu.Lock()
	defer controller.mu.Unlock()
	role, name, err := controller.role(C.GoString(path))
	if err != nil {
		_ = controller.failObservation(err) // The callback returns SQLite failure below.
		return 1034
	}
	id, err := pathIdentity(C.GoString(path))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = controller.failObservation(err) // The callback returns SQLite failure below.
		return 1034
	}
	parent, err := pathIdentity(controller.root)
	if err != nil {
		_ = controller.failObservation(err) // The callback returns SQLite failure below.
		return 1034
	}
	event := Event{Operation: "native-open", Role: role, Name: name, Actual: id, Expected: id,
		Parent: parent, Phase: "before", Result: int(result)}
	if phase == 2 {
		event.Phase = "after"
	}
	if err := controller.append(event); err != nil {
		return 1034
	}
	if controller.model == nil {
		return 0
	}
	if phase == 1 {
		controller.pending++
		return 0
	}
	if controller.pending == 0 {
		_ = controller.failObservation(ErrObservation) // The callback returns SQLite failure below.
		return 1034
	}
	controller.pending--
	if err := controller.model.scan(); err != nil {
		_ = controller.failObservation(err) // The callback returns SQLite failure below.
		return 1034
	}
	return 0
}
