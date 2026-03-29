package websocket

import (
	"errors"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConn struct {
	writeErr    error
	readPayload []byte
	readErr     error
	closeErr    error
	closed      bool
}

func (m *mockConn) WriteMessage(_ int, _ []byte) error {
	return m.writeErr
}

func (m *mockConn) ReadMessage() (int, []byte, error) {
	return websocket.TextMessage, m.readPayload, m.readErr
}

func (m *mockConn) Close() error {
	m.closed = true
	return m.closeErr
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestNew(t *testing.T) {
	t.Run("initialises config correctly", func(t *testing.T) {
		// given
		cfg := Config{URL: "ws://example.com"}

		// when
		client := New(cfg).(*clientImpl)

		// then
		assert.Equal(t, cfg, client.config)
	})

	t.Run("done channel is open on creation", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		done := client.Done()

		// then
		assert.False(t, isClosed(done))
	})

	t.Run("conn is nil before dial", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)

		// when
		conn := client.conn

		// then
		assert.Nil(t, conn)
	})
}

func TestSend(t *testing.T) {
	t.Run("returns ErrNotConnected when conn is nil", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		err := client.Send([]byte("hello"))

		// then
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("propagates write error from conn", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		writeErr := errors.New("write failed")
		client.conn = &mockConn{writeErr: writeErr}

		// when
		err := client.Send([]byte("hello"))

		// then
		assert.ErrorIs(t, err, writeErr)
	})

	t.Run("succeeds when conn is healthy", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err := client.Send([]byte("hello"))

		// then
		assert.NoError(t, err)
	})
}

func TestRead(t *testing.T) {
	t.Run("returns ErrNotConnected when conn is nil", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		msg, err := client.Read()

		// then
		assert.ErrorIs(t, err, ErrNotConnected)
		assert.Nil(t, msg)
	})

	t.Run("returns payload on success", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{readPayload: []byte("pong")}

		// when
		msg, err := client.Read()

		// then
		require.NoError(t, err)
		assert.Equal(t, []byte("pong"), msg)
	})

	t.Run("propagates read error from conn", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		readErr := errors.New("read failed")
		client.conn = &mockConn{readErr: readErr}

		// when
		msg, err := client.Read()

		// then
		assert.ErrorIs(t, err, readErr)
		assert.Nil(t, msg)
	})
}

func TestClose(t *testing.T) {
	t.Run("calls Close on conn", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		mock := &mockConn{}
		client.conn = mock

		// when
		err := client.Close()

		// then
		require.NoError(t, err)
		assert.True(t, mock.closed)
	})

	t.Run("sets conn to nil after close", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err := client.Close()

		// then
		require.NoError(t, err)
		assert.Nil(t, client.conn)
	})

	t.Run("sets conn to nil even when close returns error", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{closeErr: errors.New("close failed")}

		// when
		_ = client.Close()

		// then
		assert.Nil(t, client.conn)
	})

	t.Run("closes done channel", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err := client.Close()

		// then
		require.NoError(t, err)
		assert.True(t, isClosed(client.Done()))
	})

	t.Run("is idempotent on double close", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err1 := client.Close()
		err2 := client.Close()

		// then
		require.NoError(t, err1)
		assert.NoError(t, err2)
	})

	t.Run("succeeds when conn is already nil", func(t *testing.T) {
		// given
		client := New(Config{})
		// conn is nil from the start

		// when
		err := client.Close()

		// then
		assert.NoError(t, err)
	})

	t.Run("propagates close error from conn", func(t *testing.T) {
		// given
		closeErr := errors.New("close failed")
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{closeErr: closeErr}

		// when
		err := client.Close()

		// then
		assert.ErrorIs(t, err, closeErr)
	})
}
