package alpaca

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"trading-go/internal/domain"
	"trading-go/internal/infra/websocket"

	"github.com/shopspring/decimal"
)

type Feed struct {
	client  websocket.Client
	apiKey  string
	secret  string
	out     chan domain.MarketEvent
	symbols map[string]domain.Symbol
}

type alpacaLevel struct {
	Price decimal.Decimal `json:"p"`
	Size  decimal.Decimal `json:"s"`
}

type alpacaBookMsg struct {
	Type      string        `json:"T"`
	Symbol    string        `json:"S"`
	Timestamp string        `json:"t"`
	Bids      []alpacaLevel `json:"b"`
	Asks      []alpacaLevel `json:"a"`
	Reset     bool          `json:"r"`
}

func New(url, apiKey, secret string) *Feed {
	return &Feed{
		client:  websocket.New(websocket.Config{URL: url}),
		apiKey:  apiKey,
		secret:  secret,
		out:     make(chan domain.MarketEvent, 100),
		symbols: make(map[string]domain.Symbol),
	}
}

type alpacaControlMsg struct {
	Type string `json:"T"`
	Msg  string `json:"msg"`
	Code int    `json:"code"`
}

func (f *Feed) Connect(ctx context.Context) error {
	if err := f.client.Dial(ctx); err != nil {
		return err
	}

	// Read the "connected" welcome message Alpaca sends on connect.
	if _, err := f.client.Read(); err != nil {
		return fmt.Errorf("alpaca: reading welcome: %w", err)
	}

	auth, err := json.Marshal(map[string]any{
		"action": "auth",
		"key":    f.apiKey,
		"secret": f.secret,
	})
	if err != nil {
		return err
	}
	if err := f.client.Send(auth); err != nil {
		return err
	}

	// Wait for auth confirmation before returning so that Subscribe is never
	// sent before the server has accepted credentials.
	raw, err := f.client.Read()
	if err != nil {
		return fmt.Errorf("alpaca: reading auth response: %w", err)
	}
	var msgs []alpacaControlMsg
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return fmt.Errorf("alpaca: parsing auth response: %w", err)
	}
	for _, m := range msgs {
		if m.Type == "error" {
			return fmt.Errorf("alpaca: auth failed (code %d): %s", m.Code, m.Msg)
		}
	}

	go f.readLoop()
	return nil
}

func (f *Feed) Subscribe(pairs []domain.Symbol) error {
	symbols := make([]string, len(pairs))
	for i, p := range pairs {
		sym := p.String()
		symbols[i] = sym
		f.symbols[sym] = p
	}
	sub, err := json.Marshal(map[string]any{
		"action":     "subscribe",
		"orderbooks": symbols,
	})
	if err != nil {
		return err
	}
	return f.client.Send(sub)
}

func (f *Feed) Messages() <-chan domain.MarketEvent {
	return f.out
}

func (f *Feed) Close() error {
	return f.client.Close()
}

func (f *Feed) readLoop() {
	defer close(f.out)
	for {
		raw, err := f.client.Read()
		if err != nil {
			select {
			case <-f.client.Done():
				return
			default:
				slog.Error("alpaca read error", "error", err)
				time.Sleep(time.Second)
				continue
			}
		}

		events, err := f.toEvents(raw)
		if err != nil {
			slog.Debug("failed to parse alpaca message", "error", err, "raw", string(raw))
			continue
		}

		for _, event := range events {
			if event.Type != "" {
				f.out <- event
			}
		}
	}
}

func (f *Feed) toEvents(raw []byte) ([]domain.MarketEvent, error) {
	var msgs []alpacaBookMsg
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, err
	}

	var events []domain.MarketEvent
	for _, msg := range msgs {
		if msg.Type != "o" {
			continue
		}

		ts, _ := time.Parse(time.RFC3339Nano, msg.Timestamp)

		eventType := domain.EventOrderBook
		if msg.Reset {
			eventType = domain.EventOrderBookSnapshot
		}

		pair, ok := f.symbols[msg.Symbol]
		if !ok {
			pair = parseSymbol(msg.Symbol)
		}

		events = append(events, domain.MarketEvent{
			Type:      eventType,
			Exchange:  domain.ExchangeAlpaca,
			Pair:      pair,
			Timestamp: ts,
			Payload: domain.OrderBookDelta{
				Bids: toLevels(msg.Bids),
				Asks: toLevels(msg.Asks),
			},
		})
	}
	return events, nil
}

func parseSymbol(s string) domain.Symbol {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) == 2 {
		return domain.Symbol{Base: domain.Asset(parts[0]), Quote: domain.Asset(parts[1])}
	}
	return domain.Symbol{Base: domain.Asset(s)}
}

func toLevels(levels []alpacaLevel) []domain.Level {
	res := make([]domain.Level, len(levels))
	for i, l := range levels {
		res[i] = domain.Level{Price: l.Price, Size: l.Size}
	}
	return res
}
