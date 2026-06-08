package binance

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
	out     chan domain.MarketEvent
	symbols map[string]domain.Symbol
}

type binanceBookData struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type binanceMessage struct {
	Stream string          `json:"stream"`
	Data   binanceBookData `json:"data"`
}

func New(url string) *Feed {
	cfg := websocket.Config{
		URL: url,
	}
	return &Feed{
		client:  websocket.New(cfg),
		out:     make(chan domain.MarketEvent, 100),
		symbols: make(map[string]domain.Symbol),
	}
}

func (f *Feed) Connect(ctx context.Context) error {
	if err := f.client.Dial(ctx); err != nil {
		return err
	}
	go f.readLoop()
	return nil
}

func (f *Feed) Subscribe(pairs []domain.Symbol) error {
	params := make([]string, len(pairs))
	for i, p := range pairs {
		symbol := strings.ToLower(p.Base.String() + p.Quote.String())
		params[i] = fmt.Sprintf("%s@depth10", symbol)
		f.symbols[symbol] = p
	}

	sub := map[string]any{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     time.Now().UnixNano(),
	}

	raw, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	return f.client.Send(raw)
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
				slog.Error("binance read error", "error", err)
				time.Sleep(time.Second)
				continue
			}
		}

		event, err := f.toEvent(raw)
		if err != nil {
			slog.Debug("failed to parse binance message", "error", err, "raw", string(raw))
			continue
		}

		if event.Type != "" {
			f.out <- event
		}
	}
}

func (f *Feed) toEvent(raw []byte) (domain.MarketEvent, error) {
	var msg binanceMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		// Might be a response to SUBSCRIBE, just ignore if it's not our expected message
		return domain.MarketEvent{}, nil
	}

	if msg.Stream != "" && strings.Contains(msg.Stream, "@depth10") {
		// Format: btcusdt@depth10
		symbolRaw := strings.Split(msg.Stream, "@")[0]

		delta := domain.OrderBookDelta{
			Bids: toLevels(msg.Data.Bids),
			Asks: toLevels(msg.Data.Asks),
		}

		return domain.MarketEvent{
			Type:      domain.EventOrderBookSnapshot,
			Exchange:  domain.ExchangeBinance,
			Pair:      f.symbols[symbolRaw],
			Timestamp: time.Now(),
			Payload:   delta,
		}, nil
	}

	return domain.MarketEvent{}, nil
}

func toLevels(raw [][]string) []domain.Level {
	res := make([]domain.Level, len(raw))
	for i, r := range raw {
		if len(r) != 2 {
			continue
		}
		price, _ := decimal.NewFromString(r[0])
		size, _ := decimal.NewFromString(r[1])
		res[i] = domain.Level{
			Price: price,
			Size:  size,
		}
	}
	return res
}
