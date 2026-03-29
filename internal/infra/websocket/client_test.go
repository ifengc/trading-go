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

func Test_New(t *testing.T) {
	t.Run("initializes client with given config", func(t *testing.T) {
		// given
		cfg := Config{URL: "ws://example.com"}

		// when
		client := New(cfg).(*clientImpl)

		// then
		assert.Equal(t, cfg, client.config)
	})

	t.Run("creates an open done channel", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		done := client.Done()

		// then
		assert.False(t, isClosed(done))
	})

	t.Run("leaves connection nil before dial", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)

		// when
		conn := client.conn

		// then
		assert.Nil(t, conn)
	})
}

func Test_Client_Send(t *testing.T) {
	t.Run("returns ErrNotConnected when connection is nil", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		err := client.Send([]byte("hello"))

		// then
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("propagates write error from the connection", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		writeErr := errors.New("write failed")
		client.conn = &mockConn{writeErr: writeErr}

		// when
		err := client.Send([]byte("hello"))

		// then
		assert.ErrorIs(t, err, writeErr)
	})

	t.Run("succeeds when connection is healthy", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err := client.Send([]byte("hello"))

		// then
		assert.NoError(t, err)
	})
}

func Test_Client_Read(t *testing.T) {
	t.Run("returns ErrNotConnected when connection is nil", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		msg, err := client.Read()

		// then
		assert.ErrorIs(t, err, ErrNotConnected)
		assert.Nil(t, msg)
	})

	t.Run("returns message payload on successful read", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{readPayload: []byte("pong")}

		// when
		msg, err := client.Read()

		// then
		require.NoError(t, err)
		assert.Equal(t, []byte("pong"), msg)
	})

	t.Run("propagates read error from the connection", func(t *testing.T) {
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

func Test_Client_Close(t *testing.T) {
	t.Run("closes the underlying connection", func(t *testing.T) {
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

	t.Run("resets connection to nil after closing", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err := client.Close()

		// then
		require.NoError(t, err)
		assert.Nil(t, client.conn)
	})

	t.Run("resets connection even if close returns an error", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{closeErr: errors.New("close failed")}

		// when
		_ = client.Close()

		// then
		assert.Nil(t, client.conn)
	})

	t.Run("signals closure via the done channel", func(t *testing.T) {
		// given
		client := New(Config{}).(*clientImpl)
		client.conn = &mockConn{}

		// when
		err := client.Close()

		// then
		require.NoError(t, err)
		assert.True(t, isClosed(client.Done()))
	})

	t.Run("is idempotent when called multiple times", func(t *testing.T) {
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

	t.Run("succeeds silently when connection is already nil", func(t *testing.T) {
		// given
		client := New(Config{})

		// when
		err := client.Close()

		// then
		assert.NoError(t, err)
	})

	t.Run("propagates close error from the connection", func(t *testing.T) {
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
