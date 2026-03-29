package kraken

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"trading-go/internal/domain"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockWebsocketClient struct {
	dialErr    error
	sendErr    error
	readMsg    []byte
	readErr    error
	closeErr   error
	done       chan struct{}
	lastSent   []byte
	dialCalled bool
}

func (m *mockWebsocketClient) Dial(ctx context.Context) error {
	m.dialCalled = true
	return m.dialErr
}

func (m *mockWebsocketClient) Send(msg []byte) error {
	m.lastSent = msg
	return m.sendErr
}

func (m *mockWebsocketClient) Read() ([]byte, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	return m.readMsg, nil
}

func (m *mockWebsocketClient) Close() error {
	if m.done != nil {
		close(m.done)
	}
	return m.closeErr
}

func (m *mockWebsocketClient) Done() <-chan struct{} {
	return m.done
}

func TestNew(t *testing.T) {
	t.Run("initialises correctly", func(t *testing.T) {
		// given
		url := "ws://example.com"

		// when
		f := New(url)

		// then
		assert.NotNil(t, f)
		assert.NotNil(t, f.client)
		assert.NotNil(t, f.out)
	})
}

func TestFeed_Connect(t *testing.T) {
	t.Run("fails when dial returns error", func(t *testing.T) {
		// given
		f := New("ws://example.com")
		mock := &mockWebsocketClient{dialErr: errors.New("dial failed")}
		f.client = mock

		ctx := context.Background()

		// when
		err := f.Connect(ctx)

		// then
		assert.ErrorContains(t, err, "dial failed")
		assert.True(t, mock.dialCalled)
	})

	t.Run("succeeds when dial succeeds", func(t *testing.T) {
		// given
		f := New("ws://example.com")
		mock := &mockWebsocketClient{done: make(chan struct{})}
		f.client = mock

		// when
		err := f.Connect(context.Background())

		// then
		assert.NoError(t, err)
		assert.True(t, mock.dialCalled)
	})
}

func TestFeed_Subscribe(t *testing.T) {
	t.Run("sends correct subscription message", func(t *testing.T) {
		// given
		f := New("ws://example.com")
		mock := &mockWebsocketClient{done: make(chan struct{})}
		f.client = mock

		pair := domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

		// when
		err := f.Subscribe([]domain.Symbol{pair})

		// then
		require.NoError(t, err)

		var m map[string]any
		err = json.Unmarshal(mock.lastSent, &m)
		require.NoError(t, err)
		assert.Equal(t, "subscribe", m["method"])

		params, ok := m["params"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "book", params["channel"])

		syms, ok := params["symbol"].([]interface{})
		require.True(t, ok)
		assert.Contains(t, syms, "BTC/USDC")
	})

	t.Run("propagates send error", func(t *testing.T) {
		// given
		f := New("ws://example.com")
		mock := &mockWebsocketClient{sendErr: errors.New("send failed")}
		f.client = mock

		pair := domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

		// when
		err := f.Subscribe([]domain.Symbol{pair})

		// then
		assert.ErrorContains(t, err, "send failed")
	})
}

// Enhanced mock for streaming test
type streamingMockClient struct {
	mockWebsocketClient
	messages [][]byte
	msgIdx   int
}

func (m *streamingMockClient) Read() ([]byte, error) {
	if m.msgIdx < len(m.messages) {
		msg := m.messages[m.msgIdx]
		m.msgIdx++
		return msg, nil
	}
	// Block to simulate open connection with no new messages
	select {
	case <-m.done:
		return nil, errors.New("closed")
	case <-time.After(10 * time.Millisecond):
		// Return error to break loop in test
		return nil, errors.New("EOF")
	}
}

func TestFeed_Messages(t *testing.T) {
	t.Run("emits snapshot event", func(t *testing.T) {
		// given
		snapshotData := krakenMessage{
			Channel: "book",
			Type:    "snapshot",
			Data: []json.RawMessage{
				json.RawMessage(`{"symbol": "BTC/USD", "timestamp": "2024-03-18T12:00:00Z", "bids": [{"price": "60000.5", "qty": "1.2"}]}`),
			},
		}
		rawSnapshot, _ := json.Marshal(snapshotData)

		mock := &streamingMockClient{
			mockWebsocketClient: mockWebsocketClient{done: make(chan struct{})},
			messages:            [][]byte{rawSnapshot},
		}

		f := New("ws://example.com")
		f.client = mock

		// when
		err := f.Connect(context.Background())

		// then
		require.NoError(t, err)
		select {
		case event := <-f.Messages():
			assert.Equal(t, domain.EventOrderBookSnapshot, event.Type)
			assert.Equal(t, "kraken", event.Exchange)
			assert.Equal(t, "BTC/USD", event.Pair)

			delta, ok := event.Payload.(domain.OrderBookDelta)
			require.True(t, ok)
			require.Len(t, delta.Bids, 1)
			assert.True(t, delta.Bids[0].Price.Equal(decimal.NewFromFloat(60000.5)))
			assert.True(t, delta.Bids[0].Size.Equal(decimal.NewFromFloat(1.2)))
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for snapshot")
		}
	})
}

func TestToEvent(t *testing.T) {
	t.Run("parses valid update message", func(t *testing.T) {
		// given
		raw := []byte(`{
			"channel": "book",
			"type": "update",
			"data": [
				{
					"symbol": "ETH/USD",
					"timestamp": "2024-03-18T12:30:00Z",
					"asks": [{"price": "4000.1", "qty": "10.5"}]
				}
			]
		}`)

		// when
		event, err := toEvent(raw)

		// then
		require.NoError(t, err)
		assert.Equal(t, domain.EventOrderBook, event.Type)
		assert.Equal(t, "ETH/USD", event.Pair)
		assert.Equal(t, "kraken", event.Exchange)

		expectedTime, _ := time.Parse(time.RFC3339, "2024-03-18T12:30:00Z")
		assert.True(t, event.Timestamp.Equal(expectedTime))

		delta, ok := event.Payload.(domain.OrderBookDelta)
		require.True(t, ok)
		require.Len(t, delta.Asks, 1)
		assert.True(t, delta.Asks[0].Price.Equal(decimal.NewFromFloat(4000.1)))
		assert.True(t, delta.Asks[0].Size.Equal(decimal.NewFromFloat(10.5)))
	})

	t.Run("returns empty event for empty data", func(t *testing.T) {
		// given
		raw := []byte(`{
			"channel": "book",
			"type": "update",
			"data": []
		}`)

		// when
		event, err := toEvent(raw)

		// then
		require.NoError(t, err)
		assert.Empty(t, event.Type)
	})
}
