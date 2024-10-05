package main

import (
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"io"
	"log/slog"
	"net/http"
	"os"
)

type Storage struct {
	mux      *http.ServeMux
	name     string
	replicas []string
	leader   bool
	data     map[string]*geojson.Feature
	dataFile string
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool, dataFile string) *Storage {
	data := make(map[string]*geojson.Feature)
	s := &Storage{mux, name, replicas, leader, data, dataFile}
	s.loadFromDisk()
	s.initHandlers()
	return s
}

func (s *Storage) Run() {}

func (s *Storage) Stop() {}

func (s *Storage) initHandlers() {
	s.mux.HandleFunc("/"+s.name+"/select", s.selectHandler)
	s.mux.HandleFunc("/"+s.name+"/insert", s.insertHandler)
	s.mux.HandleFunc("/"+s.name+"/replace", s.replaceHandler)
	s.mux.HandleFunc("/"+s.name+"/delete", s.deleteHandler)
}

func (s *Storage) selectHandler(w http.ResponseWriter, _ *http.Request) {
	fc := &geojson.FeatureCollection{
		Features: make([]*geojson.Feature, 0, len(s.data)),
	}

	for _, f := range s.data {
		fc.Features = append(fc.Features, f)
	}

	data, err := json.Marshal(fc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func (s *Storage) insertHandler(w http.ResponseWriter, r *http.Request) {
	s.upsertHandler(w, r, false)
}

func (s *Storage) replaceHandler(w http.ResponseWriter, r *http.Request) {
	s.upsertHandler(w, r, true)
}

func (s *Storage) upsertHandler(w http.ResponseWriter, r *http.Request, replace bool) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if f.ID == nil {
		http.Error(w, "Missing field ID", http.StatusBadRequest)
		return
	}

	id, ok := f.ID.(string)
	if !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, exists := s.data[id]; replace && !exists {
		http.Error(w, "Feature does not exist", http.StatusNotFound)
		return
	}

	s.data[id] = f
	s.saveToDisk()

	w.WriteHeader(http.StatusOK)
}

func (s *Storage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if f.ID == nil {
		http.Error(w, "Missing field ID", http.StatusBadRequest)
		return
	}

	id, ok := f.ID.(string)
	if !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, exists := s.data[id]; !exists {
		http.Error(w, "Feature does not exist", http.StatusNotFound)
		return
	}

	delete(s.data, id)
	s.saveToDisk()

	w.WriteHeader(http.StatusOK)
}

func (s *Storage) loadFromDisk() {
	if _, err := os.Stat(s.dataFile); os.IsNotExist(err) {
		return
	}

	data, err := os.ReadFile(s.dataFile)
	if err != nil {
		slog.Error("Failed to read data from file", err)
		return
	}

	if err = json.Unmarshal(data, &s.data); err != nil {
		slog.Error("Failed to unmarshal data", err)
	}
}

func (s *Storage) saveToDisk() {
	data, err := json.Marshal(s.data)
	if err != nil {
		slog.Error("Failed to marshal data", err)
		return
	}

	if err = os.WriteFile(s.dataFile, data, 0666); err != nil {
		slog.Error("Failed to write data to file", err)
	}
}
