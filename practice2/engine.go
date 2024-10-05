package main

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"log/slog"
	"os"
)

type Engine struct {
	name         string
	data         map[string]*geojson.Feature
	rTree        *rtree.RTreeG[string]
	lsn          uint64
	commands     chan Command
	ctx          context.Context
	snapshotFile string
	walFile      string
}

func NewEngine(name string, ctx context.Context, snapshotFile string, walFile string) *Engine {
	var rTree rtree.RTreeG[string]
	return &Engine{
		name:         name,
		data:         make(map[string]*geojson.Feature),
		rTree:        &rTree,
		commands:     make(chan Command),
		ctx:          ctx,
		snapshotFile: snapshotFile,
		walFile:      walFile,
	}
}

func (e *Engine) Start() {
	_ = e.loadSnapshot()
	wal, _ := e.loadWAL()
	e.applyWAL(wal)
	e.restoreRTree()

	for {
		select {
		case <-e.ctx.Done():
			close(e.commands)
			return
		case command := <-e.commands:
			command.Execute(e)
		}
	}
}

// blocking API

func (e *Engine) GetAllData() map[string]*geojson.Feature {
	response := make(chan map[string]*geojson.Feature)
	e.commands <- &GetAllCommand{response}
	return <-response
}

func (e *Engine) GetData(coordinates [4]float64) map[string]*geojson.Feature {
	response := make(chan map[string]*geojson.Feature)
	e.commands <- &GetCommand{coordinates, response}
	return <-response
}

func (e *Engine) Exists(ID string) bool {
	response := make(chan bool)
	e.commands <- &ExistsCommand{ID, response}
	return <-response
}

func (e *Engine) ApplyTransaction(action ActionType, feature *geojson.Feature) error {
	e.lsn += 1
	tx := &Transaction{
		Action:  action,
		Name:    e.name,
		Lsn:     e.lsn,
		Feature: feature,
	}
	errors := make(chan error)
	e.commands <- &ApplyCommand{tx, errors}
	return <-errors
}

func (e *Engine) MakeSnapshot() error {
	errors := make(chan error)
	e.commands <- &SnapshotCommand{errors}
	return <-errors
}

// commands implementations

func (e *Engine) getAllData() map[string]*geojson.Feature {
	return e.data
}

func (e *Engine) getData(coordinates [4]float64) map[string]*geojson.Feature {
	minBound := [2]float64{coordinates[0], coordinates[1]} // minX, minY
	maxBound := [2]float64{coordinates[2], coordinates[3]} // maxX, maxY

	featureIDs := make([]string, 0, 32)
	e.rTree.Search(minBound, maxBound, func(_, _ [2]float64, data string) bool {
		featureIDs = append(featureIDs, data)
		return true // get all suitable features from r-tree
	})

	result := make(map[string]*geojson.Feature, len(featureIDs))
	for _, ID := range featureIDs {
		result[ID] = e.data[ID]
	}

	return result
}

func (e *Engine) applyTransaction(tx *Transaction) error {
	ID := tx.Feature.ID.(string)
	switch tx.Action {
	case Upsert:
		e.data[ID] = tx.Feature
		e.updateRTree(tx.Feature)
	case Delete:
		delete(e.data, ID)
		e.deleteFromRTree(tx.Feature)
	}
	return e.saveTransactionToWAL(tx) // blocking
}

func computeBoundsForRTree(feature *geojson.Feature) ([2]float64, [2]float64) {
	minBound := feature.Geometry.Bound().Min
	maxBound := feature.Geometry.Bound().Max
	leftBottom := [2]float64{minBound.X(), minBound.Y()}
	topRight := [2]float64{maxBound.X(), maxBound.Y()}
	return leftBottom, topRight
}

func (e *Engine) updateRTree(feature *geojson.Feature) {
	leftBottom, topRight := computeBoundsForRTree(feature)
	e.rTree.Insert(leftBottom, topRight, feature.ID.(string))
}

func (e *Engine) deleteFromRTree(feature *geojson.Feature) {
	leftBottom, topRight := computeBoundsForRTree(feature)
	e.rTree.Delete(leftBottom, topRight, feature.ID.(string))
}

func (e *Engine) makeSnapshot() error {
	if err := e.saveSnapshot(); err != nil {
		return err
	}
	return e.clearWAL()
}

// utils for load data

func (e *Engine) loadSnapshot() error {
	if _, err := os.Stat(e.snapshotFile); os.IsNotExist(err) {
		return err
	}

	data, err := os.ReadFile(e.snapshotFile)
	if err != nil {
		slog.Error("Failed to read data from snapshot", err)
		return err
	}

	if err = json.Unmarshal(data, &e.data); err != nil {
		slog.Error("Failed to unmarshal data", err)
		return err
	}

	return nil
}

func (e *Engine) loadWAL() ([]Transaction, error) {
	file, err := os.Open(e.walFile)
	if err != nil {
		if os.IsNotExist(err) {
			return []Transaction{}, nil
		}
		slog.Error("Failed to open WAL file", err)
		return nil, err
	}
	defer file.Close()

	var wal []Transaction
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tx Transaction
		line := scanner.Text()
		if err := json.Unmarshal([]byte(line), &tx); err != nil {
			slog.Error("Failed to unmarshal transaction from WAL", err)
			continue
		}
		wal = append(wal, tx)
	}

	if err := scanner.Err(); err != nil {
		slog.Error("Error reading WAL file", err)
		return nil, err
	}

	return wal, nil
}

func (e *Engine) applyWAL(wal []Transaction) {
	for _, tx := range wal {
		ID, ok := tx.Feature.ID.(string)
		if !ok {
			slog.Error("Cannot parse ID from WAL for feature", tx.Feature)
			continue
		}

		if tx.Lsn < e.lsn {
			continue // tx is already applied
		}
		e.lsn = tx.Lsn

		switch tx.Action {
		case Upsert:
			e.data[ID] = tx.Feature
		case Delete:
			delete(e.data, ID)
		default:
			slog.Warn("Unknown action in WAL", tx.Action)
		}
	}
}

func (e *Engine) restoreRTree() {
	for _, feature := range e.data {
		e.updateRTree(feature)
	}
}

// utils for save data

func (e *Engine) saveSnapshot() error {
	data, err := json.Marshal(e.data)
	if err != nil {
		slog.Error("Failed to marshal data for snapshot", err)
		return err
	}

	if err = os.WriteFile(e.snapshotFile, data, 0666); err != nil {
		slog.Error("Failed to write data to snapshot", err)
		return err
	}

	return nil
}

func (e *Engine) saveTransactionToWAL(tx *Transaction) error {
	file, err := os.OpenFile(e.walFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open the WAL file", err)
		return err
	}
	defer file.Close()

	data, err := json.Marshal(tx)
	if err != nil {
		slog.Error("Failed to serialize the transaction", tx, err)
		return err
	}

	_, err = file.Write(append(data, '\n'))
	if err != nil {
		slog.Error("Failed to save the transaction to WAL", tx, err)
		return err
	}

	return nil
}

func (e *Engine) clearWAL() error {
	file, err := os.OpenFile(e.walFile, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}
