// Idempotency demonstrates an HTTP destination with durable inventory receipts.
package main

import (
	"context"
	"net"
	"sync"
)

const maxLiveConnections = 8

type connectionListener struct {
	net.Listener
	slots chan struct{}
}

func listenLoopback(ctx context.Context) (*connectionListener, error) {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &connectionListener{
		Listener: listener, slots: make(chan struct{}, maxLiveConnections),
	}, nil
}

func (l *connectionListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &limitedConnection{Conn: conn, release: func() { <-l.slots }}, nil
		default:
			// An excess connection owns no slot or business operation.
			_ = conn.Close()
		}
	}
}

type limitedConnection struct {
	net.Conn
	once     sync.Once
	release  func()
	closeErr error
}

func (c *limitedConnection) Close() error {
	c.once.Do(func() {
		c.closeErr = c.Conn.Close()
		c.release()
	})
	return c.closeErr
}
