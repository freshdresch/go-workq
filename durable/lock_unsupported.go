//go:build !linux || !amd64

package durable

import "os"

type directoryOwner struct {
	path        string
	directory   *os.File
	parent      *os.File
	lockCreated bool
}

func acquireDirectoryObserved(string, startupObserver) (*directoryOwner, error) {
	return nil, ErrUnsupported
}

func acquireDirectory(string) (*directoryOwner, error) { return nil, ErrUnsupported }
func syncOwnedDescriptor(*os.File, string) error       { return ErrUnsupported }
func (o *directoryOwner) verify() error                { return ErrUnsupported }
func (o *directoryOwner) close() error                 { return nil }

func verifyOwnedDescriptor(*os.File, string) error { return ErrUnsupported }
