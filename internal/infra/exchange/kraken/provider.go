package kraken

import (
	"context"
	"fmt"
	"log/slog"
	"trading-go/internal/domain"
)

type OrderBookProvider struct {
	feed       domain.MarketDataFeed
	orderBooks map[string]*domain.OrderBook
	out        chan domain.OrderBookUpdate
	symbols    []domain.Symbol
}

func NewOrderBookProvider(symbols []domain.Symbol) *OrderBookProvider {
	url := "wss://ws.kraken.com/v2"
	feed := New(url)

	obs := make(map[string]*domain.OrderBook)
	for _, s := range symbols {
		obs[s.String()] = domain.NewOrderBook(s)
	}

	return &OrderBookProvider{
		feed:       feed,
		orderBooks: obs,
		out:        make(chan domain.OrderBookUpdate, 100),
		symbols:    symbols,
	}
}

func (p *OrderBookProvider) Start(ctx context.Context) error {
	if err := p.feed.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect feed: %w", err)
	}

	if err := p.feed.Subscribe(p.symbols); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	go p.readLoop(ctx)

	return nil
}

func (p *OrderBookProvider) readLoop(ctx context.Context) {
	defer close(p.out)

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-p.feed.Messages():
			if !ok {
				return
			}
			switch event.Type {
			case domain.EventOrderBookSnapshot:
				p.handleOrderBookSnapshot(event)
			case domain.EventOrderBook:
				p.handleOrderBookUpdate(event)
			}
		}
	}
}

func (p *OrderBookProvider) handleOrderBookSnapshot(event domain.MarketEvent) {
	ob, ok := p.orderBooks[event.Pair.String()]
	if !ok {
		return
	}

	delta, ok := event.Payload.(domain.OrderBookDelta)
	if !ok {
		slog.Error("unexpected payload type for snapshot", "type", fmt.Sprintf("%T", event.Payload))
		return
	}

	ob.ApplySnapshot(delta.Bids, delta.Asks)

	p.out <- domain.OrderBookUpdate{
		OrderBook: ob,
		Timestamp: event.Timestamp,
	}
}

func (p *OrderBookProvider) handleOrderBookUpdate(event domain.MarketEvent) {
	ob, ok := p.orderBooks[event.Pair.String()]
	if !ok {
		return
	}

	delta, ok := event.Payload.(domain.OrderBookDelta)
	if !ok {
		slog.Error("unexpected payload type for update", "type", fmt.Sprintf("%T", event.Payload))
		return
	}

	for _, bid := range delta.Bids {
		if err := ob.UpdateBid(bid.Price, bid.Size); err != nil {
			slog.Error("failed to update bid", "error", err, "price", bid.Price)
		}
	}

	for _, ask := range delta.Asks {
		if err := ob.UpdateAsk(ask.Price, ask.Size); err != nil {
			slog.Error("failed to update ask", "error", err, "price", ask.Price)
		}
	}

	p.out <- domain.OrderBookUpdate{
		OrderBook: ob,
		Timestamp: event.Timestamp,
	}
}

func (p *OrderBookProvider) Updates() <-chan domain.OrderBookUpdate {
	return p.out
}

func (p *OrderBookProvider) Exchange() domain.Exchange {
	return domain.ExchangeKraken
}

func (p *OrderBookProvider) Close() error {
	return p.feed.Close()
}
