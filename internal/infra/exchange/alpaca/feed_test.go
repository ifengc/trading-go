package alpaca

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
	sent       [][]byte
	dialCalled bool
}

func (m *mockWebsocketClient) Dial(_ context.Context) error {
	m.dialCalled = true
	return m.dialErr
}

func (m *mockWebsocketClient) Send(msg []byte) error {
	m.sent = append(m.sent, msg)
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
		// given / when
		f := New("ws://example.com", "key", "secret")

		// then
		assert.NotNil(t, f)
		assert.NotNil(t, f.client)
		assert.NotNil(t, f.out)
	})
}

func Test_Feed_Connect(t *testing.T) {
	t.Run("fails when dial returns error", func(t *testing.T) {
		// given
		f := New("ws://example.com", "key", "secret")
		mock := &mockWebsocketClient{dialErr: errors.New("dial failed")}
		f.client = mock

		// when
		err := f.Connect(context.Background())

		// then
		assert.ErrorContains(t, err, "dial failed")
		assert.True(t, mock.dialCalled)
	})

	t.Run("sends auth message after connecting", func(t *testing.T) {
		// given
		f := New("ws://example.com", "mykey", "mysecret")
		mock := &streamingMockClient{
			mockWebsocketClient: mockWebsocketClient{done: make(chan struct{})},
			messages: [][]byte{
				[]byte(`[{"T":"success","msg":"connected"}]`),
				[]byte(`[{"T":"success","msg":"authenticated"}]`),
			},
		}
		f.client = mock

		// when
		err := f.Connect(context.Background())

		// then
		require.NoError(t, err)
		require.Len(t, mock.sent, 1)

		var auth map[string]any
		require.NoError(t, json.Unmarshal(mock.sent[0], &auth))
		assert.Equal(t, "auth", auth["action"])
		assert.Equal(t, "mykey", auth["key"])
		assert.Equal(t, "mysecret", auth["secret"])
	})

	t.Run("returns error when auth fails", func(t *testing.T) {
		// given
		f := New("ws://example.com", "badkey", "badsecret")
		mock := &streamingMockClient{
			mockWebsocketClient: mockWebsocketClient{done: make(chan struct{})},
			messages: [][]byte{
				[]byte(`[{"T":"success","msg":"connected"}]`),
				[]byte(`[{"T":"error","code":403,"msg":"forbidden"}]`),
			},
		}
		f.client = mock

		// when
		err := f.Connect(context.Background())

		// then
		assert.ErrorContains(t, err, "auth failed")
		assert.ErrorContains(t, err, "forbidden")
	})
}

func Test_Feed_Subscribe(t *testing.T) {
	t.Run("sends correct subscription message", func(t *testing.T) {
		// given
		f := New("ws://example.com", "key", "secret")
		mock := &mockWebsocketClient{done: make(chan struct{})}
		f.client = mock

		pair := domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

		// when
		err := f.Subscribe([]domain.Symbol{pair})

		// then
		require.NoError(t, err)
		require.Len(t, mock.sent, 1)

		var m map[string]any
		require.NoError(t, json.Unmarshal(mock.sent[0], &m))
		assert.Equal(t, "subscribe", m["action"])

		obs, ok := m["orderbooks"].([]any)
		require.True(t, ok)
		assert.Contains(t, obs, "BTC/USDC")
	})

	t.Run("propagates send error", func(t *testing.T) {
		// given
		f := New("ws://example.com", "key", "secret")
		mock := &mockWebsocketClient{sendErr: errors.New("send failed")}
		f.client = mock

		// when
		err := f.Subscribe([]domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}})

		// then
		assert.ErrorContains(t, err, "send failed")
	})
}

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
	select {
	case <-m.done:
		return nil, errors.New("closed")
	case <-time.After(10 * time.Millisecond):
		return nil, errors.New("EOF")
	}
}

func Test_Feed_Messages(t *testing.T) {
	t.Run("emits snapshot event when reset message is received", func(t *testing.T) {
		// given
		raw := []byte(`[{"T":"o","S":"BTC/USDC","t":"2024-03-18T12:00:00Z","b":[{"p":"60000.5","s":"1.2"}],"a":[],"r":true}]`)

		mock := &streamingMockClient{
			mockWebsocketClient: mockWebsocketClient{done: make(chan struct{})},
			messages: [][]byte{
				[]byte(`[{"T":"success","msg":"connected"}]`),
				[]byte(`[{"T":"success","msg":"authenticated"}]`),
				raw,
			},
		}

		f := New("ws://example.com", "key", "secret")
		f.client = mock
		f.symbols["BTC/USDC"] = domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

		// when
		err := f.Connect(context.Background())

		// then
		require.NoError(t, err)
		select {
		case event := <-f.Messages():
			assert.Equal(t, domain.EventOrderBookSnapshot, event.Type)
			assert.Equal(t, domain.ExchangeAlpaca, event.Exchange)
			assert.Equal(t, domain.Symbol{Base: domain.BTC, Quote: domain.USDC}, event.Pair)

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

func Test_ToEvents(t *testing.T) {
	t.Run("parses snapshot message (r=true)", func(t *testing.T) {
		// given
		raw := []byte(`[{"T":"o","S":"BTC/USDC","t":"2024-03-18T12:00:00Z","b":[{"p":"60000","s":"1.5"}],"a":[{"p":"60100","s":"2.0"}],"r":true}]`)
		f := New("ws://example.com", "key", "secret")
		f.symbols["BTC/USDC"] = domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

		// when
		events, err := f.toEvents(raw)

		// then
		require.NoError(t, err)
		require.Len(t, events, 1)

		event := events[0]
		assert.Equal(t, domain.EventOrderBookSnapshot, event.Type)
		assert.Equal(t, domain.ExchangeAlpaca, event.Exchange)
		assert.Equal(t, domain.Symbol{Base: domain.BTC, Quote: domain.USDC}, event.Pair)

		expectedTime, _ := time.Parse(time.RFC3339, "2024-03-18T12:00:00Z")
		assert.True(t, event.Timestamp.Equal(expectedTime))

		delta, ok := event.Payload.(domain.OrderBookDelta)
		require.True(t, ok)
		require.Len(t, delta.Bids, 1)
		assert.True(t, delta.Bids[0].Price.Equal(decimal.NewFromFloat(60000)))
		assert.True(t, delta.Bids[0].Size.Equal(decimal.NewFromFloat(1.5)))
		require.Len(t, delta.Asks, 1)
		assert.True(t, delta.Asks[0].Price.Equal(decimal.NewFromFloat(60100)))
		assert.True(t, delta.Asks[0].Size.Equal(decimal.NewFromFloat(2.0)))
	})

	t.Run("parses incremental update message (r=false)", func(t *testing.T) {
		// given
		raw := []byte(`[{"T":"o","S":"ETH/USDC","t":"2024-03-18T12:30:00Z","b":[{"p":"4000","s":"0.5"}],"a":[],"r":false}]`)
		f := New("ws://example.com", "key", "secret")
		f.symbols["ETH/USDC"] = domain.Symbol{Base: domain.ETH, Quote: domain.USDC}

		// when
		events, err := f.toEvents(raw)

		// then
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, domain.EventOrderBook, events[0].Type)
		assert.Equal(t, domain.Symbol{Base: domain.ETH, Quote: domain.USDC}, events[0].Pair)
	})

	t.Run("skips non-orderbook messages", func(t *testing.T) {
		// given
		raw := []byte(`[{"T":"success","msg":"authenticated"}]`)
		f := New("ws://example.com", "key", "secret")

		// when
		events, err := f.toEvents(raw)

		// then
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("parses symbol from message when not in symbols map", func(t *testing.T) {
		// given
		raw := []byte(`[{"T":"o","S":"BTC/USDT","t":"2024-03-18T12:00:00Z","b":[],"a":[],"r":true}]`)
		f := New("ws://example.com", "key", "secret")

		// when
		events, err := f.toEvents(raw)

		// then
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, domain.Symbol{Base: domain.BTC, Quote: domain.USDT}, events[0].Pair)
	})
}
