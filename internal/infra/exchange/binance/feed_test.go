package binance

import (
	"context"
	"encoding/json"
	"testing"

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

func (m *mockWebsocketClient) Dial(_ context.Context) error {
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

func Test_New(t *testing.T) {
	t.Run("initializes feed correctly", func(t *testing.T) {
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

func Test_Feed_Subscribe(t *testing.T) {
	t.Run("sends correct subscription message", func(t *testing.T) {
		// given
		f := New("ws://example.com")
		mock := &mockWebsocketClient{done: make(chan struct{})}
		f.client = mock

		pair := domain.Symbol{Base: domain.BTC, Quote: domain.USDT}

		// when
		err := f.Subscribe([]domain.Symbol{pair})

		// then
		require.NoError(t, err)

		var m map[string]any
		err = json.Unmarshal(mock.lastSent, &m)
		require.NoError(t, err)
		assert.Equal(t, "SUBSCRIBE", m["method"])

		params, ok := m["params"].([]any)
		require.True(t, ok)
		assert.Contains(t, params, "btcusdt@depth10")
	})
}

func Test_ToEvent(t *testing.T) {
	t.Run("parses valid depth10 message", func(t *testing.T) {
		// given
		raw := []byte(`{
			"stream": "btcusdt@depth10",
			"data": {
				"lastUpdateId": 160,
				"bids": [["60000.5", "1.2"]],
				"asks": [["60000.6", "0.5"]]
			}
		}`)

		f := New("ws://example.com")
		f.symbols["btcusdt"] = domain.Symbol{Base: domain.BTC, Quote: domain.USDT}

		// when
		event, err := f.toEvent(raw)

		// then
		require.NoError(t, err)
		assert.Equal(t, domain.EventOrderBookSnapshot, event.Type)
		assert.Equal(t, domain.Symbol{Base: domain.BTC, Quote: domain.USDT}, event.Pair)
		assert.Equal(t, domain.ExchangeBinance, event.Exchange)

		delta, ok := event.Payload.(domain.OrderBookDelta)
		require.True(t, ok)
		require.Len(t, delta.Bids, 1)
		assert.True(t, delta.Bids[0].Price.Equal(decimal.NewFromFloat(60000.5)))
		assert.True(t, delta.Bids[0].Size.Equal(decimal.NewFromFloat(1.2)))
		require.Len(t, delta.Asks, 1)
		assert.True(t, delta.Asks[0].Price.Equal(decimal.NewFromFloat(60000.6)))
		assert.True(t, delta.Asks[0].Size.Equal(decimal.NewFromFloat(0.5)))
	})

	t.Run("returns empty event for unexpected message", func(t *testing.T) {
		// given
		raw := []byte(`{"result":null,"id":1}`)
		f := New("ws://example.com")

		// when
		event, err := f.toEvent(raw)

		// then
		require.NoError(t, err)
		assert.Empty(t, event.Type)
	})
}
