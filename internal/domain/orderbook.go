package domain

import (
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/shopspring/decimal"
)

type Asset string

const (
	BTC  Asset = "BTC"
	ETH  Asset = "ETH"
	USDC Asset = "USDC"
	USDT Asset = "USDT"
)

// TODO: confirm efficient priceDegree values
const priceDegree = 32

func (a Asset) String() string {
	return string(a)
}

type Symbol struct {
	Base  Asset
	Quote Asset
}

func (s Symbol) String() string {
	return s.Base.String() + "/" + s.Quote.String()
}

type Price = decimal.Decimal
type Size = decimal.Decimal
type Level struct {
	Price Price
	Size  Size
}

type OrderBook struct {
	mu     sync.RWMutex
	Symbol Symbol
	Bids   *btree.BTreeG[Level]
	Asks   *btree.BTreeG[Level]
}

func NewOrderBook(symbol Symbol) *OrderBook {
	return &OrderBook{
		Symbol: symbol,
		Bids:   btree.NewG(priceDegree, priceComparator),
		Asks:   btree.NewG(priceDegree, priceComparator),
	}
}

func priceComparator(a, b Level) bool {
	return a.Price.LessThan(b.Price)
}

func (ob *OrderBook) UpdateBid(price Price, size Size) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	level := Level{Price: price, Size: size}
	if size.IsPositive() {
		ob.Bids.ReplaceOrInsert(level)
	} else if size.IsNegative() {
		return fmt.Errorf("Negative size is not allowed for UpdateBid: %s", size)
	} else {
		ob.Bids.Delete(level)
	}
	return nil
}

func (ob *OrderBook) UpdateAsk(price Price, size Size) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	level := Level{Price: price, Size: size}
	if size.IsPositive() {
		ob.Asks.ReplaceOrInsert(level)
	} else if size.IsNegative() {
		return fmt.Errorf("Negative size is not allowed for UpdateAsk: %s", size)
	} else {
		ob.Asks.Delete(level)
	}
	return nil
}

func (ob *OrderBook) bestBid() *Level {
	if level, exists := ob.Bids.Max(); exists {
		return &level
	}
	return nil
}

func (ob *OrderBook) BestBid() *Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	return ob.bestBid()
}

func (ob *OrderBook) BestBids(n int) []Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if n <= 0 {
		return nil
	}
	levels := make([]Level, 0, n)
	ob.Bids.Descend(func(l Level) bool {
		levels = append(levels, l)
		return len(levels) < n
	})
	return levels
}

func (ob *OrderBook) bestAsk() *Level {
	if level, exists := ob.Asks.Min(); exists {
		return &level
	}
	return nil
}

func (ob *OrderBook) BestAsk() *Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	return ob.bestAsk()
}

func (ob *OrderBook) BestAsks(n int) []Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if n <= 0 {
		return nil
	}
	levels := make([]Level, 0, n)
	ob.Asks.Ascend(func(l Level) bool {
		levels = append(levels, l)
		return len(levels) < n
	})
	return levels
}

func (ob *OrderBook) ApplySnapshot(bids []Level, asks []Level) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	ob.Bids.Clear(true)
	ob.Asks.Clear(true)

	for _, level := range bids {
		if level.Size.IsPositive() {
			ob.Bids.ReplaceOrInsert(level)
		}
	}
	for _, level := range asks {
		if level.Size.IsPositive() {
			ob.Asks.ReplaceOrInsert(level)
		}
	}
}

func (ob *OrderBook) Spread() *Price {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	bestBid := ob.bestBid()
	bestAsk := ob.bestAsk()
	if bestBid == nil || bestAsk == nil {
		return nil
	}
	spread := bestAsk.Price.Sub(bestBid.Price)
	return &spread
}

func (ob *OrderBook) IsCrossed() bool {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	bestBid := ob.bestBid()
	bestAsk := ob.bestAsk()
	if bestBid == nil || bestAsk == nil {
		return false
	}
	return bestBid.Price.GreaterThanOrEqual(bestAsk.Price)
}
