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

func gracefulShutdown(storages []*Storage, router *Router, l *http.Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	slog.Info("Got signal", sig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, storage := range storages {
		storage.Stop()
	}
	router.Stop()
	_ = l.Shutdown(ctx)
}

func main() {
	mux := http.ServeMux{}

	storages := []*Storage{
		NewStorage(&mux, "storage-1-1", []string{"storage-1-2", "storage-1-3", "storage-1-4"}, true, "../data/1/1/snapshot.json", "../data/1/1/wal.txt"),
		NewStorage(&mux, "storage-1-2", []string{"storage-1-1", "storage-1-3", "storage-1-4"}, false, "../data/1/2/snapshot.json", "../data/1/2/wal.txt"),
		NewStorage(&mux, "storage-1-3", []string{"storage-1-1", "storage-1-2", "storage-1-4"}, false, "../data/1/3/snapshot.json", "../data/1/3/wal.txt"),
		NewStorage(&mux, "storage-1-4", []string{"storage-1-1", "storage-1-2", "storage-1-3"}, false, "../data/1/4/snapshot.json", "../data/1/4/wal.txt"),
	}
	storageNames := make([]string, 0)
	for _, storage := range storages {
		storageNames = append(storageNames, storage.name)
	}

	router := NewRouter(&mux, [][]string{storageNames}, [][]string{{"storage-1-1"}}, "../front/dist")
	server := http.Server{Addr: "127.0.0.1:8080", Handler: &mux}

	for _, storage := range storages {
		go storage.Run()
	}
	go router.Run()
	go gracefulShutdown(storages, router, &server)

	slog.Info("Listen http://" + server.Addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Fatal error", err)
	}
}
