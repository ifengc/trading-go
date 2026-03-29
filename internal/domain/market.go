package domain

import (
	"context"
	"time"
)

type EventType string

const (
	EventOrderBook         EventType = "orderbook"
	EventOrderBookSnapshot EventType = "orderbook_snapshot"
	EventTrade             EventType = "trade"
	EventTicker            EventType = "ticker"
)

type MarketEvent struct {
	Type      EventType
	Exchange  string
	Pair      string
	Timestamp time.Time
	Payload   any
}

type MarketDataFeed interface {
	Connect(ctx context.Context) error
	Subscribe(pairs []Symbol) error
	Messages() <-chan MarketEvent
	Close() error
}
