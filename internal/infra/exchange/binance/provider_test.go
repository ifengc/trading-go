package binance

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
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDT}}

		// when
		p := NewOrderBookProvider(symbols)

		// then
		assert.NotNil(t, p)
		assert.NotNil(t, p.feed)
		assert.NotNil(t, p.orderBooks)
		assert.Contains(t, p.orderBooks, "BTC/USDT")
	})
}

func Test_OrderBookProvider_HandleSnapshot(t *testing.T) {
	t.Run("updates order book state from snapshot", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDT}}
		p := NewOrderBookProvider(symbols)

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
			Pair:      domain.Symbol{Base: domain.BTC, Quote: domain.USDT},
			Timestamp: time.Now(),
			Payload:   delta,
		}

		// when
		p.handleOrderBookSnapshot(event)

		// then
		ob, ok := p.orderBooks["BTC/USDT"]
		require.True(t, ok)
		require.NotNil(t, ob)

		bestBid := ob.BestBid()
		require.NotNil(t, bestBid)
		assert.True(t, bestBid.Price.Equal(decimal.NewFromFloat(60000)))

		// check output channel
		select {
		case update := <-p.Updates():
			assert.Equal(t, "BTC/USDT", update.OrderBook.Symbol.String())
		default:
			t.Fatal("expected update on channel")
		}
	})
}

func Test_OrderBookProvider_HandleUpdate(t *testing.T) {
	t.Run("modifies existing order book levels", func(t *testing.T) {
		// given
		symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDT}}
		p := NewOrderBookProvider(symbols)

		ob := p.orderBooks["BTC/USDT"]
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
			Pair:      domain.Symbol{Base: domain.BTC, Quote: domain.USDT},
			Timestamp: time.Now(),
			Payload:   delta,
		}

		// when
		p.handleOrderBookUpdate(event)

		// then
		level, ok := ob.Bids.Get(domain.Level{Price: decimal.NewFromFloat(59900)})
		require.True(t, ok)
		assert.True(t, level.Size.Equal(decimal.NewFromFloat(2.0)))
	})
}
