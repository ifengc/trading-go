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

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

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

	currentBooks := make(map[string]domain.OrderBookUpdate)
	var mu sync.Mutex

	go func() {
		for {
			select {
			case update, ok := <-krakenProvider.Updates():
				if !ok {
					return
				}
				mu.Lock()
				currentBooks["Kraken"] = update
				displayOrderBooks(currentBooks)
				mu.Unlock()
			case update, ok := <-binanceProvider.Updates():
				if !ok {
					return
				}
				mu.Lock()
				currentBooks["Binance"] = update
				displayOrderBooks(currentBooks)
				mu.Unlock()
			case update, ok := <-alpacaProvider.Updates():
				if !ok {
					return
				}
				mu.Lock()
				currentBooks["Alpaca"] = update
				displayOrderBooks(currentBooks)
				mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
	fmt.Println("\nShutting down...")
}
