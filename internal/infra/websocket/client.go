package websocket

import (
	"context"
	"errors"
	"sync"

	"github.com/gorilla/websocket"
)

var ErrNotConnected = errors.New("websocket: not connected")

type Config struct {
	URL string
	// TODO: Support more options like headers, ping/pong interval, retries, backoff etc.
}

type Conn interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

type Client interface {
	Dial(ctx context.Context) error
	Send(msg []byte) error
	Read() ([]byte, error)
	Close() error
	Done() <-chan struct{}
}

type clientImpl struct {
	config Config
	conn   Conn
	mu     sync.Mutex
	done   chan struct{}
}

func New(cfg Config) Client {
	return &clientImpl{
		config: cfg,
		done:   make(chan struct{}),
	}
}

func (c *clientImpl) Done() <-chan struct{} {
	return c.done
}

func (c *clientImpl) Dial(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.config.URL, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

func (c *clientImpl) Send(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return ErrNotConnected
	}
	return c.conn.WriteMessage(websocket.TextMessage, msg)
}

func (c *clientImpl) Read() ([]byte, error) {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return nil, ErrNotConnected
	}
	_, p, err := conn.ReadMessage()
	return p, err
}

func (c *clientImpl) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
	}

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}
