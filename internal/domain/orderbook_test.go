package domain

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewOrderBook(t *testing.T) {
	t.Run("initializes with empty bids and asks", func(t *testing.T) {
		// given
		symbol := Symbol{Base: BTC, Quote: USDT}

		// when
		ob := NewOrderBook(symbol)

		// then
		assert.NotNil(t, ob)
		assert.Equal(t, symbol, ob.Symbol)

		assert.NotNil(t, ob.Bids)
		assert.Equal(t, 0, ob.Bids.Len())

		assert.NotNil(t, ob.Asks)
		assert.Equal(t, 0, ob.Asks.Len())
	})
}

func Test_UpdateBid(t *testing.T) {
	t.Run("adds new price level", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)
		size := decimal.NewFromFloat(1.5)

		// when
		err := ob.UpdateBid(price, size)

		// then
		require.NoError(t, err)
		assert.NotNil(t, ob.BestBid())
		assert.Equal(t, price, ob.BestBid().Price)
		assert.Equal(t, size, ob.BestBid().Size)
	})

	t.Run("replaces size for existing price", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)
		_ = ob.UpdateBid(price, decimal.NewFromFloat(1.0))

		// when
		err := ob.UpdateBid(price, decimal.NewFromFloat(2.5))

		// then
		require.NoError(t, err)
		assert.Equal(t, decimal.NewFromFloat(2.5), ob.BestBid().Size)
	})

	t.Run("removes level when size is zero", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)
		_ = ob.UpdateBid(price, decimal.NewFromFloat(1.0))

		// when
		err := ob.UpdateBid(price, decimal.NewFromFloat(0))

		// then
		require.NoError(t, err)
		assert.Nil(t, ob.BestBid())
	})

	t.Run("returns error for negative size", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)

		// when
		err := ob.UpdateBid(price, decimal.NewFromFloat(-1.0))

		// then
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Negative size is not allowed for UpdateBid")
	})
}

func Test_UpdateAsk(t *testing.T) {
	t.Run("adds new price level", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)
		size := decimal.NewFromFloat(1.5)

		// when
		err := ob.UpdateAsk(price, size)

		// then
		require.NoError(t, err)
		assert.NotNil(t, ob.BestAsk())
		assert.Equal(t, price, ob.BestAsk().Price)
		assert.Equal(t, size, ob.BestAsk().Size)
	})

	t.Run("replaces size for existing price", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)
		_ = ob.UpdateAsk(price, decimal.NewFromFloat(1.0))

		// when
		err := ob.UpdateAsk(price, decimal.NewFromFloat(2.5))

		// then
		require.NoError(t, err)
		assert.Equal(t, decimal.NewFromFloat(2.5), ob.BestAsk().Size)
	})

	t.Run("removes level when size is zero", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)
		_ = ob.UpdateAsk(price, decimal.NewFromFloat(1.0))

		// when
		err := ob.UpdateAsk(price, decimal.NewFromFloat(0))

		// then
		require.NoError(t, err)
		assert.Nil(t, ob.BestAsk())
	})

	t.Run("returns error for negative size", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		price := decimal.NewFromFloat(100.0)

		// when
		err := ob.UpdateAsk(price, decimal.NewFromFloat(-1.0))

		// then
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Negative size is not allowed for UpdateAsk")
	})
}

func Test_BestBidAndAsk(t *testing.T) {
	t.Run("returns nil for empty book", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})

		// when
		bid := ob.BestBid()
		ask := ob.BestAsk()

		// then
		assert.Nil(t, bid)
		assert.Nil(t, ask)
	})
}

func Test_BestBids(t *testing.T) {
	t.Run("returns top N levels in descending order", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateBid(decimal.NewFromFloat(102.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateBid(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		// when
		levels := ob.BestBids(2)

		// then
		assert.Len(t, levels, 2)
		assert.Equal(t, decimal.NewFromFloat(102.0), levels[0].Price)
		assert.Equal(t, decimal.NewFromFloat(101.0), levels[1].Price)
	})

	t.Run("returns nil for n <= 0", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))

		// when
		levels0 := ob.BestBids(0)
		levelsNeg := ob.BestBids(-1)

		// then
		assert.Nil(t, levels0)
		assert.Nil(t, levelsNeg)
	})
}

func Test_BestAsks(t *testing.T) {
	t.Run("returns top N levels in ascending order", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateAsk(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(98.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(99.0), decimal.NewFromFloat(1.0))

		// when
		levels := ob.BestAsks(2)

		// then
		assert.Len(t, levels, 2)
		assert.Equal(t, decimal.NewFromFloat(98.0), levels[0].Price)
		assert.Equal(t, decimal.NewFromFloat(99.0), levels[1].Price)
	})

	t.Run("returns nil for n <= 0", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateAsk(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))

		// when
		levels0 := ob.BestAsks(0)
		levelsNeg := ob.BestAsks(-1)

		// then
		assert.Nil(t, levels0)
		assert.Nil(t, levelsNeg)
	})
}

func Test_ApplySnapshot(t *testing.T) {
	t.Run("replaces current state", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		newBids := []Level{
			{Price: decimal.NewFromFloat(98.0), Size: decimal.NewFromFloat(2.0)},
			{Price: decimal.NewFromFloat(99.0), Size: decimal.NewFromFloat(3.0)},
		}
		newAsks := []Level{
			{Price: decimal.NewFromFloat(102.0), Size: decimal.NewFromFloat(4.0)},
		}

		// when
		ob.ApplySnapshot(newBids, newAsks)

		// then
		assert.Equal(t, 2, ob.Bids.Len())
		assert.Equal(t, 1, ob.Asks.Len())

		bids := ob.BestBids(2)
		assert.Equal(t, decimal.NewFromFloat(99.0), bids[0].Price)
		assert.Equal(t, decimal.NewFromFloat(3.0), bids[0].Size)

		asks := ob.BestAsks(1)
		assert.Equal(t, decimal.NewFromFloat(102.0), asks[0].Price)
	})

	t.Run("handles empty bids", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		// when
		ob.ApplySnapshot([]Level{}, []Level{{Price: decimal.NewFromFloat(102.0), Size: decimal.NewFromFloat(4.0)}})

		// then
		assert.Equal(t, 0, ob.Bids.Len())
		assert.Equal(t, 1, ob.Asks.Len())
		assert.Nil(t, ob.BestBid())
	})

	t.Run("handles empty asks", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		// when
		ob.ApplySnapshot([]Level{{Price: decimal.NewFromFloat(98.0), Size: decimal.NewFromFloat(2.0)}}, []Level{})

		// then
		assert.Equal(t, 1, ob.Bids.Len())
		assert.Equal(t, 0, ob.Asks.Len())
		assert.Nil(t, ob.BestAsk())
	})

	t.Run("handles both empty", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		// when
		ob.ApplySnapshot([]Level{}, []Level{})

		// then
		assert.Equal(t, 0, ob.Bids.Len())
		assert.Equal(t, 0, ob.Asks.Len())
	})
}

func Test_Spread(t *testing.T) {
	t.Run("returns nil for empty book", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})

		// when
		spread := ob.Spread()

		// then
		assert.Nil(t, spread)
	})

	t.Run("returns nil for book with only bids", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))

		// when
		spread := ob.Spread()

		// then
		assert.Nil(t, spread)
	})

	t.Run("returns nil for book with only asks", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		// when
		spread := ob.Spread()

		// then
		assert.Nil(t, spread)
	})

	t.Run("returns correct spread", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.5), decimal.NewFromFloat(1.0))

		// when
		spread := ob.Spread()

		// then
		require.NotNil(t, spread)
		assert.True(t, decimal.NewFromFloat(1.5).Equal(*spread))
	})
}

func Test_IsCrossed(t *testing.T) {
	t.Run("returns false for empty book", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})

		// when
		crossed := ob.IsCrossed()

		// then
		assert.False(t, crossed)
	})

	t.Run("returns false when not crossed", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))

		// when
		crossed := ob.IsCrossed()

		// then
		assert.False(t, crossed)
	})

	t.Run("returns true when crossed", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(101.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))

		// when
		crossed := ob.IsCrossed()

		// then
		assert.True(t, crossed)
	})

	t.Run("returns true when locked (best bid == best ask)", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		_ = ob.UpdateBid(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))
		_ = ob.UpdateAsk(decimal.NewFromFloat(100.0), decimal.NewFromFloat(1.0))

		// when
		crossed := ob.IsCrossed()

		// then
		assert.True(t, crossed)
	})
}

func Test_OrderBook_Concurrency(t *testing.T) {
	t.Run("handles concurrent updates and reads", func(t *testing.T) {
		// given
		ob := NewOrderBook(Symbol{Base: BTC, Quote: USDT})
		iterations := 100
		workers := 4

		done := make(chan bool)

		// when/then
		for i := 0; i < workers; i++ {
			go func(workerID int) {
				for j := 0; j < iterations; j++ {
					price := decimal.NewFromInt(int64(j))
					size := decimal.NewFromInt(int64(workerID + 1))

					_ = ob.UpdateBid(price, size)
					_ = ob.UpdateAsk(price.Add(decimal.NewFromInt(10)), size)

					_ = ob.BestBid()
					_ = ob.BestAsk()
					_ = ob.Spread()
					_ = ob.IsCrossed()
				}
				done <- true
			}(i)
		}

		for i := 0; i < workers; i++ {
			<-done
		}
	})
}
