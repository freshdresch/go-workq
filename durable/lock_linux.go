//go:build linux && amd64

package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const maxDirectoryBytes = 4095

type directoryOwner struct {
	path        string
	directory   *os.File
	lock        *os.File
	parent      *os.File
	lockCreated bool
}

func acquireDirectory(path string) (*directoryOwner, error) {
	return acquireDirectoryObserved(path, nil)
}

func acquireDirectoryObserved(
	path string,
	observer startupObserver,
) (_ *directoryOwner, err error) {
	return acquireDirectoryMode(path, observer, true)
}

func acquireExistingDirectory(path string) (*directoryOwner, error) {
	return acquireDirectoryMode(path, nil, false)
}

func acquireDirectoryMode(
	path string,
	observer startupObserver,
	create bool,
) (_ *directoryOwner, err error) {
	// Bound caller-controlled cleaning before filepath.Abs can allocate from its length.
	if len(path) > maxDirectoryBytes {
		return nil, ErrInvalidInput
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if len(absolute) > maxDirectoryBytes {
		return nil, ErrInvalidInput
	}
	parentPath, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, err
	}
	if len(parentPath) > maxDirectoryBytes {
		return nil, ErrInvalidInput
	}
	canonical := filepath.Join(parentPath, filepath.Base(absolute))
	if len(canonical) > maxDirectoryBytes {
		return nil, ErrInvalidInput
	}
	if create {
		if err := os.Mkdir(canonical, 0700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return nil, err
			}
		}
	}
	canonical, err = filepath.EvalSymlinks(canonical)
	if err != nil {
		return nil, err
	}
	if len(canonical) > maxDirectoryBytes {
		return nil, ErrInvalidInput
	}
	if observer != nil {
		observer.Observe(startupDirectoryReady)
	}
	owner := &directoryOwner{path: canonical}
	defer func() {
		if err != nil {
			err = errors.Join(err, owner.close())
		}
	}()
	owner.directory, err = openOwnedFile(canonical, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	owner.parent, err = openOwnedFile(filepath.Dir(canonical), syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	lockFlags := syscall.O_RDWR
	if create {
		lockFlags |= syscall.O_CREAT | syscall.O_EXCL
	}
	owner.lock, err = openOwnedFile(filepath.Join(canonical, "LOCK"), lockFlags, 0600)
	if create && err == nil {
		owner.lockCreated = true
	}
	if create && errors.Is(err, syscall.EEXIST) {
		owner.lock, err = openOwnedFile(filepath.Join(canonical, "LOCK"), syscall.O_RDWR, 0)
	}
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(owner.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrInUse
		}
		return nil, err
	}
	if err = owner.verify(); err != nil {
		return nil, err
	}
	return owner, nil
}

func openOwnedFile(path string, flags int, mode uint32) (*os.File, error) {
	descriptor, err := syscall.Open(path, flags|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, mode)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), path), nil
}

func verifyOwnedDescriptor(file *os.File, path string) error {
	actual, err := file.Stat()
	if err != nil {
		return err
	}
	expected, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(actual, expected) {
		return fmt.Errorf("identity changed: %w", ErrStorage)
	}
	return nil
}

// The retained descriptor, rather than a newly resolved path, is the sync authority.
func syncOwnedDescriptor(file *os.File, path string) error {
	if err := verifyOwnedDescriptor(file, path); err != nil {
		return err
	}
	after, err := observeOwnedSync(file, path)
	if err == nil {
		err = file.Sync()
	}
	return after(err)
}

func (o *directoryOwner) verify() error {
	if err := verifyOwnedDescriptor(o.directory, o.path); err != nil {
		return err
	}
	return verifyOwnedDescriptor(o.lock, filepath.Join(o.path, "LOCK"))
}

func (o *directoryOwner) close() error {
	var err error
	for _, file := range []*os.File{o.parent, o.directory} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	if o.lock != nil {
		// A forked child can retain this open description until exec despite CLOEXEC.
		// Explicit unlock ends ownership after this owner's other handles have closed.
		err = errors.Join(err, syscall.Flock(int(o.lock.Fd()), syscall.LOCK_UN))
		err = errors.Join(err, o.lock.Close())
	}
	return err
}
