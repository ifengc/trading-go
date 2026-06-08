# trading-go

Real-time cryptocurrency order book aggregator. Connects to multiple exchange WebSocket feeds and renders the books in an ANSI terminal display — either side by side per exchange or merged into a single unified view.

## Exchanges

- **Kraken** — WebSocket v2, incremental updates
- **Binance** — `@depth10` stream, snapshot-only updates
- **Alpaca** — Crypto data stream (authenticated), incremental + reset snapshots

Alpaca requires credentials. Set `ALPACA_API_KEY` and `ALPACA_API_SECRET` in the environment or in a `.env` file (loaded automatically).

## Running

```bash
# Per-exchange order books side by side (default)
go run ./cmd/terminal/
go run ./cmd/terminal/ split

# Unified book merged across exchanges, with per-exchange contributions
go run ./cmd/terminal/ aggregated
```

## Building

```bash
go build -o bin/trading-go-terminal ./cmd/terminal/
```

## Testing

```bash
go test ./...
go test ./internal/domain/...
go test ./internal/infra/exchange/kraken/...
```

