package kraken

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
	"trading-go/internal/domain"
	"trading-go/internal/infra/websocket"
)

type Feed struct {
	client websocket.Client
	out    chan domain.MarketEvent
}

func New(url string) *Feed {
	cfg := websocket.Config{
		URL: url,
	}
	return &Feed{
		client: websocket.New(cfg),
		out:    make(chan domain.MarketEvent, 100),
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
	symbols := make([]string, len(pairs))
	for i, p := range pairs {
		symbols[i] = p.String()
	}
	sub := map[string]any{
		"method": "subscribe",
		"params": map[string]any{
			"channel": "book",
			"symbol":  symbols,
			"depth":   10,
		},
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
				slog.Error("kraken read error", "error", err)
				// Basic backoff or wait might be needed here to avoid tight loop on persistent errors
				time.Sleep(time.Second)
				continue
			}
		}

		event, err := toEvent(raw)
		if err != nil {
			slog.Debug("failed to parse kraken message", "error", err, "raw", string(raw))
			continue
		}

		if event.Type != "" {
			f.out <- event
		}
	}
}

type krakenMessage struct {
	Channel string            `json:"channel"`
	Type    string            `json:"type"`
	Data    []json.RawMessage `json:"data"`
}

type krakenBookData struct {
	Symbol    string `json:"symbol"`
	Timestamp string `json:"timestamp"`
}

func toEvent(raw []byte) (domain.MarketEvent, error) {
	var msg krakenMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return domain.MarketEvent{}, err
	}

	if (msg.Type == "update" || msg.Type == "snapshot") && msg.Channel == "book" {
		if len(msg.Data) == 0 {
			return domain.MarketEvent{}, nil
		}

		var data krakenBookData
		if err := json.Unmarshal(msg.Data[0], &data); err != nil {
			return domain.MarketEvent{}, err
		}

		ts, _ := time.Parse(time.RFC3339, data.Timestamp)

		eventType := domain.EventOrderBook
		if msg.Type == "snapshot" {
			eventType = domain.EventOrderBookSnapshot
		}

		return domain.MarketEvent{
			Type:      eventType,
			Exchange:  "kraken",
			Pair:      data.Symbol,
			Timestamp: ts,
			Payload:   msg.Data[0],
		}, nil
	}

	return domain.MarketEvent{}, nil
}
