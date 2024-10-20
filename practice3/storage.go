package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

type Storage struct {
	mux         *http.ServeMux
	name        string
	replicas    []string
	leader      bool
	engine      *Engine
	ctx         context.Context
	cancel      context.CancelFunc
	upgrader    websocket.Upgrader
	connections *ReplicaRegistry
	curSelects  int32
}

const MaxRedirects int32 = 3

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool, snapshotFile string, walFile string) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	engine := NewEngine(name, replicas, ctx, snapshotFile, walFile)
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	connections := NewReplicaRegistry(name)
	return &Storage{mux, name, replicas, leader, engine, ctx, cancel, upgrader, connections, 0}
}

func (s *Storage) Run() {
	s.initHandlers()
	go s.engine.Start()
}

func (s *Storage) Stop() {
	s.cancel()
}

func (s *Storage) initHandlers() {
	s.mux.HandleFunc("/"+s.name+"/select", s.selectHandler)
	s.mux.HandleFunc("/"+s.name+"/insert", s.insertHandler)
	s.mux.HandleFunc("/"+s.name+"/replace", s.replaceHandler)
	s.mux.HandleFunc("/"+s.name+"/delete", s.deleteHandler)
	s.mux.HandleFunc("/"+s.name+"/snapshot", s.snapshotHandler)
	s.mux.HandleFunc("/"+s.name+"/replication", s.replicationHandler)
}

func (s *Storage) replicationHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Upgrade error", err)
		return
	}

	replica := r.URL.Query().Get("name")
	s.connections.Add(replica, conn)

	go func() {
		defer conn.Close()
		defer s.connections.Remove(replica)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				slog.Error("Read from (another) leader "+replica+" error, me: "+s.name, err)
				return
			}

			var tx Transaction
			if err := json.Unmarshal(message, &tx); err != nil {
				slog.Error("Failed to unmarshal transaction from replica "+replica, err)
				return
			}

			if err := s.engine.ApplyTransactionRaw(&tx); err != nil {
				slog.Error(fmt.Sprintf("Failed to apply transaction %v from replica", tx), err)
			}
		}
	}()
}

func (s *Storage) redirectIfNeeded(w http.ResponseWriter, r *http.Request) bool {
	if s.curSelects < MaxRedirects {
		return false
	}

	ttl, err := strconv.Atoi(r.URL.Query().Get("ttl"))
	if err != nil {
		ttl = int(MaxRedirects)
	}
	if ttl <= 0 {
		http.Error(w, "TTL is 0", http.StatusTooManyRequests)
		return true
	}
	r.URL.Query().Set("ttl", strconv.Itoa(ttl-1))

	replica := s.replicas[rand.IntN(len(s.replicas))]
	targetURL := &url.URL{Path: "/" + replica + "/select", RawQuery: r.URL.RawQuery}
	http.Redirect(w, r, targetURL.String(), http.StatusTemporaryRedirect)

	return true
}

func (s *Storage) selectHandler(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&s.curSelects, 1)
	defer atomic.AddInt32(&s.curSelects, -1)

	if s.redirectIfNeeded(w, r) {
		return
	}

	rectParam := r.URL.Query().Get("rect")

	var data map[string]*geojson.Feature
	if rectParam == "" {
		data = s.engine.GetAllData()
	} else {
		coordinates, err := parseRectParam(rectParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data = s.engine.GetData(coordinates)
	}

	fc := &geojson.FeatureCollection{
		Features: make([]*geojson.Feature, 0, len(data)),
	}

	for _, f := range data {
		fc.Features = append(fc.Features, f)
	}

	bytes, err := json.Marshal(fc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(bytes); err != nil {
		slog.Error("Failed to respond with all features", err)
	}
}

func (s *Storage) insertHandler(w http.ResponseWriter, r *http.Request) {
	s.upsertHandler(w, r, false)
}

func (s *Storage) replaceHandler(w http.ResponseWriter, r *http.Request) {
	s.upsertHandler(w, r, true)
}

func (s *Storage) upsertHandler(w http.ResponseWriter, r *http.Request, replace bool) {
	if !s.leader {
		slog.Warn("Current node " + s.name + " is not a leader")
		return
	}

	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	feature, err := geojson.UnmarshalFeature(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if feature.ID == nil {
		http.Error(w, "Missing field ID", http.StatusBadRequest)
		return
	}

	ID, ok := feature.ID.(string)
	if !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if replace && !s.engine.Exists(ID) {
		http.Error(w, "Feature does not exist", http.StatusNotFound)
		return
	}

	if err := s.engine.ApplyTransaction(Upsert, feature); err != nil {
		http.Error(w, "Failed to save feature", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Storage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if !s.leader {
		slog.Warn("Current node " + s.name + " is not a leader")
		return
	}

	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	feature, err := geojson.UnmarshalFeature(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if feature.ID == nil {
		http.Error(w, "Missing field ID", http.StatusBadRequest)
		return
	}

	ID, ok := feature.ID.(string)
	if !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !s.engine.Exists(ID) {
		http.Error(w, "Feature does not exist", http.StatusNotFound)
		return
	}

	if err := s.engine.ApplyTransaction(Delete, feature); err != nil {
		http.Error(w, "Failed to delete feature", http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Storage) snapshotHandler(w http.ResponseWriter, _ *http.Request) {
	if err := s.engine.MakeSnapshot(); err != nil {
		http.Error(w, "Failed to make snapshot", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// utils

func parseRectParam(rectParam string) ([4]float64, error) {
	coordinates := strings.Split(rectParam, ",")
	if len(coordinates) != 4 {
		return [4]float64{}, fmt.Errorf("rect parameter must contain exactly 4 values")
	}

	var result [4]float64
	for i, str := range coordinates {
		value, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return [4]float64{}, err
		}
		result[i] = value
	}

	return result, nil
}
