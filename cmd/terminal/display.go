package main

import (
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"
	"trading-go/internal/domain"
)

func displayOrderBooks(books map[string]domain.OrderBookUpdate) {
	if len(books) == 0 {
		return
	}

	// Clear screen
	fmt.Print("\033[H\033[2J")

	exchanges := make([]string, 0, len(books))
	for name := range books {
		exchanges = append(exchanges, name)
	}
	sort.Strings(exchanges)

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

		// Spread
		spread := ob.Spread()
		if spread != nil {
			fmt.Fprintf(w, "Spread\t%s\t\n", spread.String())
		} else {
			fmt.Fprintln(w, "Spread\t-\t")
		}

		// Bids
		bids := ob.BestBids(depth)
		for _, b := range bids {
			fmt.Fprintf(w, "\033[32mBids\033[0m\t%s\t%s\n", b.Price.String(), b.Size.String())
		}

		w.Flush()
		fmt.Println() // Space between exchanges
	}
}
