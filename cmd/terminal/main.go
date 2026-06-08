package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"trading-go/internal/domain"
	"trading-go/internal/infra/exchange/alpaca"
	"trading-go/internal/infra/exchange/binance"
	"trading-go/internal/infra/exchange/kraken"
	"trading-go/internal/service"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	mode := parseMode()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	alpacaKey := os.Getenv("ALPACA_API_KEY")
	alpacaSecret := os.Getenv("ALPACA_API_SECRET")
	if alpacaKey == "" || alpacaSecret == "" {
		slog.Error("ALPACA_API_KEY and ALPACA_API_SECRET must be set")
		os.Exit(1)
	}

	symbols := []domain.Symbol{{Base: domain.BTC, Quote: domain.USDT}}

	krakenProvider := kraken.NewOrderBookProvider(symbols)
	binanceProvider := binance.NewOrderBookProvider(symbols)
	alpacaProvider := alpaca.NewOrderBookProvider(symbols, alpacaKey, alpacaSecret)

	if err := krakenProvider.Start(ctx); err != nil {
		slog.Error("failed to start kraken provider", "error", err)
		os.Exit(1)
	}
	defer krakenProvider.Close()

	if err := binanceProvider.Start(ctx); err != nil {
		slog.Error("failed to start binance provider", "error", err)
		os.Exit(1)
	}
	defer binanceProvider.Close()

	if err := alpacaProvider.Start(ctx); err != nil {
		slog.Error("failed to start alpaca provider", "error", err)
		os.Exit(1)
	}
	defer alpacaProvider.Close()

	updates := fanIn(ctx, []service.Provider{krakenProvider, binanceProvider, alpacaProvider})

	switch mode {
	case "split":
		runSplit(ctx, updates)
	case "aggregated":
		runAggregated(ctx, updates, symbols[0])
	}

	fmt.Println("\nShutting down...")
}

type taggedUpdate struct {
	exchange domain.Exchange
	update   domain.OrderBookUpdate
}

// fanIn merges every provider's Updates() into a single channel, tagging each update with its
// exchange. The returned channel is closed once every provider channel drains or ctx is cancelled.
func fanIn(ctx context.Context, providers []service.Provider) <-chan taggedUpdate {
	out := make(chan taggedUpdate, 100)
	var wg sync.WaitGroup
	for _, p := range providers {
		wg.Add(1)
		go func(p service.Provider) {
			defer wg.Done()
			for {
				select {
				case u, ok := <-p.Updates():
					if !ok {
						return
					}
					select {
					case out <- taggedUpdate{exchange: p.Exchange(), update: u}:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(p)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func runSplit(ctx context.Context, updates <-chan taggedUpdate) {
	books := make(map[domain.Exchange]domain.OrderBookUpdate)
	for {
		select {
		case t, ok := <-updates:
			if !ok {
				return
			}
			books[t.exchange] = t.update
			displayOrderBooks(books)
		case <-ctx.Done():
			return
		}
	}
}

func parseMode() string {
	mode := "split"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "split", "aggregated":
		return mode
	default:
		fmt.Fprintf(os.Stderr, "usage: terminal [split|aggregated]\n")
		fmt.Fprintf(os.Stderr, "  split       per-exchange order books side by side (default)\n")
		fmt.Fprintf(os.Stderr, "  aggregated  unified book merged across exchanges\n")
		os.Exit(2)
	}
	return mode
}

func runAggregated(ctx context.Context, updates <-chan taggedUpdate, symbol domain.Symbol) {
	agg := service.NewAggregator(symbol)

	go func() {
		for {
			select {
			case t, ok := <-updates:
				if !ok {
					return
				}
				agg.Apply(t.exchange, t.update)
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case snapshot, ok := <-agg.Updates():
			if !ok {
				return
			}
			displayAggregatedBook(snapshot)
		case <-ctx.Done():
			return
		}
	}
}
