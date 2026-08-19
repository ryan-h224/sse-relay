// Command sse-relay runs the HTTP server described in the README: producers
// POST chunks to a stream id, browsers subscribe over SSE, and a client that
// reconnects with Last-Event-ID resumes from the replay buffer instead of
// missing history.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ryan-h224/sse-relay/internal/hub"
	"github.com/ryan-h224/sse-relay/internal/relay"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sse-relay:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("sse-relay", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "listen address")
	buffer := fs.Int("buffer", hub.DefaultCapacity, "events kept per stream for replay")
	heartbeat := fs.Duration("heartbeat", 15*time.Second, "delay between heartbeat comment frames")
	retry := fs.Duration("retry", 2*time.Second, "reconnect delay advertised in the retry field")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests on shutdown")
	if err := fs.Parse(args); err != nil {
		return err
	}

	h := hub.New(*buffer)
	handler := relay.NewServer(h, relay.Config{
		Heartbeat: *heartbeat,
		RetryHint: *retry,
		Token:     os.Getenv("RELAY_TOKEN"),
	})

	srv := &http.Server{
		Addr:    *addr,
		Handler: handler,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("sse-relay listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		return err
	case s := <-sig:
		log.Printf("received %s, shutting down", s)
	}

	// Finish every stream before the listener closes, so subscribers still
	// connected receive `event: done` instead of a severed connection.
	h.CloseAll()

	ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	return srv.Shutdown(ctx)
}
