package domain

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
)

type Asset string

const (
	BTC  Asset = "BTC"
	ETH  Asset = "ETH"
	USDC Asset = "USDC"
	USDT Asset = "USDT"
)

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

type OrderBookDelta struct {
	Bids []Level
	Asks []Level
}

type OrderBookUpdate struct {
	OrderBook *OrderBook
	Timestamp time.Time
}

type EventType string

const (
	EventOrderBook         EventType = "orderbook"
	EventOrderBookSnapshot EventType = "orderbook_snapshot"
	EventTrade             EventType = "trade"
	EventTicker            EventType = "ticker"
)

type Exchange string

const (
	ExchangeAlpaca  Exchange = "alpaca"
	ExchangeBinance Exchange = "binance"
	ExchangeKraken  Exchange = "kraken"
)

type MarketEvent struct {
	Type      EventType
	Exchange  Exchange
	Pair      Symbol
	Timestamp time.Time
	Payload   any
}

type MarketDataFeed interface {
	Connect(ctx context.Context) error
	Subscribe(pairs []Symbol) error
	Messages() <-chan MarketEvent
	Close() error
}

type ExchangeContribution struct {
	Exchange Exchange
	Size     Size
}

type AggregatedLevel struct {
	Price         Price
	TotalSize     Size
	Contributions []ExchangeContribution
}

type AggregatedOrderBookUpdate struct {
	Symbol    Symbol
	Bids      []AggregatedLevel // sorted best (highest price) first
	Asks      []AggregatedLevel // sorted best (lowest price) first
	Timestamp time.Time
}
