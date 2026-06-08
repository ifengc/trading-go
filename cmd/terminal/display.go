package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
	"trading-go/internal/domain"
)

func displayOrderBooks(books map[domain.Exchange]domain.OrderBookUpdate) {
	if len(books) == 0 {
		return
	}

	// Clear screen
	fmt.Print("\033[H\033[2J")

	exchanges := make([]domain.Exchange, 0, len(books))
	for name := range books {
		exchanges = append(exchanges, name)
	}
	sort.Slice(exchanges, func(i, j int) bool { return exchanges[i] < exchanges[j] })

	for _, name := range exchanges {
		update := books[name]
		ob := update.OrderBook
		lag := time.Since(update.Timestamp).Round(time.Millisecond)

		fmt.Printf("--- %s (%s) [Lag: %s] ---\n", name, ob.Symbol, lag)

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "Type\tPrice\tSize")

		depth := 10

		// Asks (highest price at top)
		asks := ob.BestAsks(depth)
		for i := 0; i < len(asks)/2; i++ {
			j := len(asks) - 1 - i
			asks[i], asks[j] = asks[j], asks[i]
		}
		for _, a := range asks {
			fmt.Fprintf(w, "\033[31mAsks\033[0m\t%s\t%s\n", a.Price.String(), a.Size.String())
		}

		spread := ob.Spread()
		if spread != nil {
			fmt.Fprintf(w, "Spread\t%s\t\n", spread.String())
		} else {
			fmt.Fprintln(w, "Spread\t-\t")
		}

		bids := ob.BestBids(depth)
		for _, b := range bids {
			fmt.Fprintf(w, "\033[32mBids\033[0m\t%s\t%s\n", b.Price.String(), b.Size.String())
		}

		w.Flush()
		fmt.Println() // Space between exchanges
	}
}

func displayAggregatedBook(book domain.AggregatedOrderBookUpdate) {
	// Clear screen
	fmt.Print("\033[H\033[2J")

	lag := time.Since(book.Timestamp).Round(time.Millisecond)
	fmt.Printf("=== Aggregated (%s) [Lag: %s] ===\n", book.Symbol, lag)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Type\tPrice\tTotalSize\tSources")

	depth := 10

	// Compute spread from the best levels before we reorder anything for display.
	var spread *domain.Price
	if len(book.Asks) > 0 && len(book.Bids) > 0 {
		s := book.Asks[0].Price.Sub(book.Bids[0].Price)
		spread = &s
	}

	// Asks (lowest price first in the snapshot); show highest price at the top.
	asks := book.Asks
	if len(asks) > depth {
		asks = asks[:depth]
	}
	for i := 0; i < len(asks)/2; i++ {
		j := len(asks) - 1 - i
		asks[i], asks[j] = asks[j], asks[i]
	}
	for _, a := range asks {
		fmt.Fprintf(w, "\033[31mAsks\033[0m\t%s\t%s\t%s\n", a.Price.String(), a.TotalSize.String(), formatSources(a.Contributions))
	}

	if spread != nil {
		fmt.Fprintf(w, "Spread\t%s\t\t\n", spread.String())
	} else {
		fmt.Fprintln(w, "Spread\t-\t\t")
	}

	// Bids (highest price first in the snapshot).
	bids := book.Bids
	if len(bids) > depth {
		bids = bids[:depth]
	}
	for _, b := range bids {
		fmt.Fprintf(w, "\033[32mBids\033[0m\t%s\t%s\t%s\n", b.Price.String(), b.TotalSize.String(), formatSources(b.Contributions))
	}

	w.Flush()
	fmt.Println()
}

func formatSources(contributions []domain.ExchangeContribution) string {
	parts := make([]string, 0, len(contributions))
	for _, c := range contributions {
		parts = append(parts, fmt.Sprintf("%s:%s", c.Exchange, c.Size.String()))
	}
	return strings.Join(parts, " ")
}
