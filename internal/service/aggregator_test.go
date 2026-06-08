package service

import (
	"testing"
	"time"

	"trading-go/internal/domain"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func level(price, size float64) domain.Level {
	return domain.Level{Price: decimal.NewFromFloat(price), Size: decimal.NewFromFloat(size)}
}

func bookWith(symbol domain.Symbol, bids, asks []domain.Level) *domain.OrderBook {
	ob := domain.NewOrderBook(symbol)
	ob.ApplySnapshot(bids, asks)
	return ob
}

func Test_Aggregator_Snapshot(t *testing.T) {
	symbol := domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

	t.Run("merges levels across exchanges with per-exchange contributions and correct ordering", func(t *testing.T) {
		// given: two exchanges quoting the same symbol, sharing some price levels
		agg := NewAggregator(symbol)

		kraken := bookWith(symbol,
			[]domain.Level{level(60000, 1.0), level(59900, 2.0)},
			[]domain.Level{level(60100, 1.0)},
		)
		binance := bookWith(symbol,
			[]domain.Level{level(60000, 0.5), level(59800, 1.0)},
			[]domain.Level{level(60100, 2.0), level(60200, 1.0)},
		)

		// when
		agg.Apply(domain.ExchangeKraken, domain.OrderBookUpdate{OrderBook: kraken, Timestamp: time.Now()})
		agg.Apply(domain.ExchangeBinance, domain.OrderBookUpdate{OrderBook: binance, Timestamp: time.Now()})
		merged := agg.Snapshot()

		// then: bids highest-first, with shared level summed and contributions in stable order
		require.Len(t, merged.Bids, 3)
		assert.True(t, merged.Bids[0].Price.Equal(decimal.NewFromFloat(60000)))
		assert.True(t, merged.Bids[1].Price.Equal(decimal.NewFromFloat(59900)))
		assert.True(t, merged.Bids[2].Price.Equal(decimal.NewFromFloat(59800)))

		assert.True(t, merged.Bids[0].TotalSize.Equal(decimal.NewFromFloat(1.5)))
		// contributions are appended in exchange-sorted order: binance before kraken
		require.Len(t, merged.Bids[0].Contributions, 2)
		assert.Equal(t, domain.ExchangeBinance, merged.Bids[0].Contributions[0].Exchange)
		assert.True(t, merged.Bids[0].Contributions[0].Size.Equal(decimal.NewFromFloat(0.5)))
		assert.Equal(t, domain.ExchangeKraken, merged.Bids[0].Contributions[1].Exchange)
		assert.True(t, merged.Bids[0].Contributions[1].Size.Equal(decimal.NewFromFloat(1.0)))

		// then: asks lowest-first, shared level summed
		require.Len(t, merged.Asks, 2)
		assert.True(t, merged.Asks[0].Price.Equal(decimal.NewFromFloat(60100)))
		assert.True(t, merged.Asks[1].Price.Equal(decimal.NewFromFloat(60200)))
		assert.True(t, merged.Asks[0].TotalSize.Equal(decimal.NewFromFloat(3.0)))
		require.Len(t, merged.Asks[0].Contributions, 2)

		assert.Equal(t, symbol, merged.Symbol)
	})

	t.Run("returns a frozen snapshot that does not reflect later book mutations", func(t *testing.T) {
		// given
		agg := NewAggregator(symbol)
		kraken := bookWith(symbol, []domain.Level{level(60000, 1.0)}, nil)
		agg.Apply(domain.ExchangeKraken, domain.OrderBookUpdate{OrderBook: kraken, Timestamp: time.Now()})

		// when
		frozen := agg.Snapshot()
		require.NoError(t, kraken.UpdateBid(decimal.NewFromFloat(60000), decimal.NewFromFloat(5.0)))
		fresh := agg.Snapshot()

		// then
		require.Len(t, frozen.Bids, 1)
		assert.True(t, frozen.Bids[0].TotalSize.Equal(decimal.NewFromFloat(1.0)))
		require.Len(t, fresh.Bids, 1)
		assert.True(t, fresh.Bids[0].TotalSize.Equal(decimal.NewFromFloat(5.0)))
	})

	t.Run("empty aggregator yields an empty snapshot", func(t *testing.T) {
		// given
		agg := NewAggregator(symbol)

		// when
		merged := agg.Snapshot()

		// then
		assert.Empty(t, merged.Bids)
		assert.Empty(t, merged.Asks)
	})
}

func Test_Aggregator_Apply(t *testing.T) {
	symbol := domain.Symbol{Base: domain.BTC, Quote: domain.USDC}

	t.Run("emits the merged view on Updates with the event timestamp", func(t *testing.T) {
		// given
		agg := NewAggregator(symbol)
		ts := time.Now()
		kraken := bookWith(symbol, []domain.Level{level(60000, 1.0)}, []domain.Level{level(60100, 1.0)})

		// when
		agg.Apply(domain.ExchangeKraken, domain.OrderBookUpdate{OrderBook: kraken, Timestamp: ts})

		// then
		select {
		case update := <-agg.Updates():
			assert.Equal(t, symbol, update.Symbol)
			assert.Equal(t, ts, update.Timestamp)
			require.Len(t, update.Bids, 1)
			assert.True(t, update.Bids[0].Price.Equal(decimal.NewFromFloat(60000)))
			require.Len(t, update.Asks, 1)
			assert.True(t, update.Asks[0].Price.Equal(decimal.NewFromFloat(60100)))
		default:
			t.Fatal("expected an aggregated update on the channel")
		}
	})

	t.Run("drops an update whose symbol does not match the aggregator's", func(t *testing.T) {
		// given
		agg := NewAggregator(symbol)
		wrongSymbol := domain.Symbol{Base: domain.ETH, Quote: domain.USDC}
		ethBook := bookWith(wrongSymbol, []domain.Level{level(3000, 1.0)}, []domain.Level{level(3001, 1.0)})

		// when
		agg.Apply(domain.ExchangeKraken, domain.OrderBookUpdate{OrderBook: ethBook, Timestamp: time.Now()})

		// then
		select {
		case <-agg.Updates():
			t.Fatal("expected the mismatched update to be dropped, but one was emitted")
		default:
		}
		merged := agg.Snapshot()
		assert.Empty(t, merged.Bids)
		assert.Empty(t, merged.Asks)
	})
}
