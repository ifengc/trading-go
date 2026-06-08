package alpaca

import (
	"context"
	"testing"
	"time"

	"trading-go/internal/domain"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFeed struct {
	connectCalled   bool
	subscribeCalled bool
	closedCalled    bool
	connectErr      error
	subscribeErr    error
	messages        chan domain.MarketEvent
}

func (m *mockFeed) Connect(_ context.Context) error {
	m.connectCalled = true
	return m.connectErr
}

func (m *mockFeed) Subscribe(_ []domain.Symbol) error {
	m.subscribeCalled = true
	return m.subscribeErr
}

func (m *mockFeed) Messages() <-chan domain.MarketEvent {
	return m.messages
}

func (m *mockFeed) Close() error {
	m.closedCalled = true
	return nil
}

func Test_NewOrderBookProvider(t *testing.T) {
	t.Run("initializes correctly with given symbols", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}}

		// when
		p := NewOrderBookProvider(symbols, "key", "secret")

		// then
		assert.NotNil(t, p)
		assert.NotNil(t, p.feed)
		assert.NotNil(t, p.orderBooks)
		assert.Contains(t, p.orderBooks, "BTC/USDC")
	})
}

func Test_OrderBookProvider_Start(t *testing.T) {
	t.Run("connects and subscribes to the feed", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}}
		p := NewOrderBookProvider(symbols, "key", "secret")

		mock := &mockFeed{messages: make(chan domain.MarketEvent)}
		p.feed = mock

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// when
		err := p.Start(ctx)

		// then
		assert.NoError(t, err)
		assert.True(t, mock.connectCalled)
		assert.True(t, mock.subscribeCalled)
	})
}

func Test_OrderBookProvider_HandleSnapshot(t *testing.T) {
	t.Run("updates order book state from snapshot", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}}
		p := NewOrderBookProvider(symbols, "key", "secret")

		delta := domain.OrderBookDelta{
			Bids: []domain.Level{
				{Price: decimal.NewFromFloat(60000), Size: decimal.NewFromFloat(1.5)},
			},
			Asks: []domain.Level{
				{Price: decimal.NewFromFloat(60100), Size: decimal.NewFromFloat(2.5)},
			},
		}

		event := domain.MarketEvent{
			Type:      domain.EventOrderBookSnapshot,
			Pair:      domain.Symbol{Base: domain.BTC, Quote: domain.USDC},
			Timestamp: time.Now(),
			Payload:   delta,
		}

		// when
		p.handleOrderBookSnapshot(event)

		// then
		ob, ok := p.orderBooks["BTC/USDC"]
		require.True(t, ok)

		bestBid := ob.BestBid()
		require.NotNil(t, bestBid)
		assert.True(t, bestBid.Price.Equal(decimal.NewFromFloat(60000)))
		assert.True(t, bestBid.Size.Equal(decimal.NewFromFloat(1.5)))

		bestAsk := ob.BestAsk()
		require.NotNil(t, bestAsk)
		assert.True(t, bestAsk.Price.Equal(decimal.NewFromFloat(60100)))
		assert.True(t, bestAsk.Size.Equal(decimal.NewFromFloat(2.5)))

		select {
		case update := <-p.Updates():
			assert.Equal(t, "BTC/USDC", update.OrderBook.Symbol.String())
		default:
			t.Fatal("expected update on channel")
		}
	})
}

func Test_OrderBookProvider_HandleUpdate(t *testing.T) {
	t.Run("modifies existing order book levels", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}}
		p := NewOrderBookProvider(symbols, "key", "secret")

		ob := p.orderBooks["BTC/USDC"]
		ob.ApplySnapshot(
			[]domain.Level{{Price: decimal.NewFromFloat(60000), Size: decimal.NewFromFloat(1.0)}},
			[]domain.Level{{Price: decimal.NewFromFloat(60100), Size: decimal.NewFromFloat(1.0)}},
		)

		delta := domain.OrderBookDelta{
			Bids: []domain.Level{
				{Price: decimal.NewFromFloat(60000), Size: decimal.Zero},
				{Price: decimal.NewFromFloat(59900), Size: decimal.NewFromFloat(2.0)},
			},
			Asks: []domain.Level{
				{Price: decimal.NewFromFloat(60100), Size: decimal.NewFromFloat(1.2)},
			},
		}

		event := domain.MarketEvent{
			Type:      domain.EventOrderBook,
			Pair:      domain.Symbol{Base: domain.BTC, Quote: domain.USDC},
			Timestamp: time.Now(),
			Payload:   delta,
		}

		// when
		p.handleOrderBookUpdate(event)

		// then
		_, ok := ob.Bids.Get(domain.Level{Price: decimal.NewFromFloat(60000)})
		assert.False(t, ok)

		level, ok := ob.Bids.Get(domain.Level{Price: decimal.NewFromFloat(59900)})
		require.True(t, ok)
		assert.True(t, level.Size.Equal(decimal.NewFromFloat(2.0)))

		level, ok = ob.Asks.Get(domain.Level{Price: decimal.NewFromFloat(60100)})
		require.True(t, ok)
		assert.True(t, level.Size.Equal(decimal.NewFromFloat(1.2)))
	})
}

func Test_OrderBookProvider_ReadLoop(t *testing.T) {
	t.Run("processes messages and shuts down on context cancel", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}}
		p := NewOrderBookProvider(symbols, "key", "secret")

		mock := &mockFeed{messages: make(chan domain.MarketEvent, 1)}
		p.feed = mock

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go p.readLoop(ctx)

		event := domain.MarketEvent{
			Type:      domain.EventOrderBookSnapshot,
			Pair:      domain.Symbol{Base: domain.BTC, Quote: domain.USDC},
			Timestamp: time.Now(),
			Payload: domain.OrderBookDelta{
				Bids: []domain.Level{{Price: decimal.NewFromFloat(60000), Size: decimal.NewFromFloat(1)}},
			},
		}

		// when
		mock.messages <- event

		// then
		select {
		case update := <-p.Updates():
			assert.Equal(t, "BTC/USDC", update.OrderBook.Symbol.String())
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for update")
		}

		// when: shutdown
		cancel()

		// then: output channel eventually closed
		timeout := time.After(time.Second)
		for {
			select {
			case _, ok := <-p.Updates():
				if !ok {
					return
				}
			case <-timeout:
				t.Fatal("timed out waiting for Updates channel to close")
			}
		}
	})
}

func Test_OrderBookProvider_Close(t *testing.T) {
	t.Run("shuts down the underlying feed", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDC}}
		p := NewOrderBookProvider(symbols, "key", "secret")

		mock := &mockFeed{}
		p.feed = mock

		// when
		err := p.Close()

		// then
		assert.NoError(t, err)
		assert.True(t, mock.closedCalled)
	})
}

func Test_OrderBookProvider_Exchange(t *testing.T) {
	t.Run("returns alpaca exchange", func(t *testing.T) {
		// given
		p := NewOrderBookProvider(nil, "key", "secret")

		// when / then
		assert.Equal(t, domain.ExchangeAlpaca, p.Exchange())
	})
}
