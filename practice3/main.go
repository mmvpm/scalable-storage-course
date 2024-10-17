package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func gracefulShutdown(storage *Storage, router *Router, l *http.Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	slog.Info("Got signal", sig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	storage.Stop()
	router.Stop()
	_ = l.Shutdown(ctx)
}

func main() {
	mux := http.ServeMux{}
	storage := NewStorage(&mux, "storage", []string{}, true, "../data/snapshot.json", "../data/wal.txt")
	router := NewRouter(&mux, [][]string{{storage.name}}, "../front/dist")
	server := http.Server{Addr: "127.0.0.1:8080", Handler: &mux}

	go storage.Run()
	go router.Run()
	go gracefulShutdown(storage, router, &server)

	slog.Info("Listen http://" + server.Addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Fatal error", err)
	}
}
