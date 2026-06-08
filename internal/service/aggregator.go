package service

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	"trading-go/internal/domain"
)

const levelsPerExchange = 20

type Provider interface {
	Updates() <-chan domain.OrderBookUpdate
	Exchange() domain.Exchange
}

// Aggregator merges order book updates from multiple providers into a single unified view.
// One Aggregator handles exactly one Symbol, fixed at construction.
// It is not goroutine-safe for Apply — call Apply from a single goroutine (e.g. the fan-in loop).
// Snapshot is safe to call concurrently from other goroutines.
type Aggregator struct {
	mu          sync.RWMutex
	books       map[domain.Exchange]*domain.OrderBook
	symbol      domain.Symbol
	out         chan domain.AggregatedOrderBookUpdate
	logMismatch sync.Once
}

func NewAggregator(symbol domain.Symbol) *Aggregator {
	return &Aggregator{
		symbol: symbol,
		books:  make(map[domain.Exchange]*domain.OrderBook),
		out:    make(chan domain.AggregatedOrderBookUpdate, 100),
	}
}

func (a *Aggregator) Apply(exchange domain.Exchange, update domain.OrderBookUpdate) {
	// An update for a different symbol is a wiring bug, not a normal event: merging two symbols'
	// books by raw price would corrupt the aggregated view, so drop it. The mismatch is static
	// (the symbol set is fixed at startup), so it would recur every tick — log only once to
	// surface the bug without flooding.
	if update.OrderBook.Symbol != a.symbol {
		a.logMismatch.Do(func() {
			slog.Error("aggregator received update for unexpected symbol; dropping",
				"exchange", exchange, "expected", a.symbol, "got", update.OrderBook.Symbol)
		})
		return
	}

	a.mu.Lock()
	a.books[exchange] = update.OrderBook
	a.mu.Unlock()

	merged := a.Snapshot()
	merged.Timestamp = update.Timestamp

	select {
	case a.out <- merged:
	default:
		// drop if the consumer is slow; each emit is a full snapshot, so no state is lost.
	}
}

// Snapshot returns a frozen, internally consistent merged view of the aggregated book. Each
// exchange's top-N levels are captured once and the merged view is built purely from those captured
// values — the live order books are never re-read while merging — so the result is safe to walk for
// execution pricing. Safe to call concurrently.
func (a *Aggregator) Snapshot() domain.AggregatedOrderBookUpdate {
	bids, asks, symbol := a.capture()
	return domain.AggregatedOrderBookUpdate{
		Symbol:    symbol,
		Bids:      mergeLevels(bids, true),
		Asks:      mergeLevels(asks, false),
		Timestamp: time.Now(),
	}
}

func (a *Aggregator) capture() (bids, asks map[domain.Exchange][]domain.Level, symbol domain.Symbol) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	bids = make(map[domain.Exchange][]domain.Level, len(a.books))
	asks = make(map[domain.Exchange][]domain.Level, len(a.books))
	for ex, ob := range a.books {
		bids[ex] = ob.BestBids(levelsPerExchange)
		asks[ex] = ob.BestAsks(levelsPerExchange)
	}
	return bids, asks, a.symbol
}

// mergeLevels merges captured per-exchange levels into aggregated levels. It is pure: it reads no
// order books. Exchanges are visited in a stable (sorted) order so per-level Contributions are
// deterministic. Bids are returned highest-price first, asks lowest-price first.
func mergeLevels(captured map[domain.Exchange][]domain.Level, isBid bool) []domain.AggregatedLevel {
	exchanges := make([]domain.Exchange, 0, len(captured))
	for ex := range captured {
		exchanges = append(exchanges, ex)
	}
	sort.Slice(exchanges, func(i, j int) bool { return exchanges[i] < exchanges[j] })

	grouped := make(map[string]*domain.AggregatedLevel)
	for _, ex := range exchanges {
		for _, l := range captured[ex] {
			key := l.Price.String()
			agg, exists := grouped[key]
			if !exists {
				agg = &domain.AggregatedLevel{Price: l.Price}
				grouped[key] = agg
			}
			agg.TotalSize = agg.TotalSize.Add(l.Size)
			agg.Contributions = append(agg.Contributions, domain.ExchangeContribution{Exchange: ex, Size: l.Size})
		}
	}

	result := make([]domain.AggregatedLevel, 0, len(grouped))
	for _, agg := range grouped {
		result = append(result, *agg)
	}
	if isBid {
		sort.Slice(result, func(i, j int) bool { return result[i].Price.GreaterThan(result[j].Price) })
	} else {
		sort.Slice(result, func(i, j int) bool { return result[i].Price.LessThan(result[j].Price) })
	}
	return result
}

func (a *Aggregator) Updates() <-chan domain.AggregatedOrderBookUpdate {
	return a.out
}
