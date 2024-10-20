package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
)

type Engine struct {
	name         string
	replicas     []string
	connections  *ReplicaRegistry
	data         map[string]*Feature
	rTree        *rtree.RTreeG[string]
	vclock       map[string]uint64
	commands     chan Command
	ctx          context.Context
	snapshotFile string
	walFile      string
}

func NewEngine(name string, replicas []string, ctx context.Context, snapshotFile string, walFile string) *Engine {
	var rTree rtree.RTreeG[string]
	return &Engine{
		name:         name,
		replicas:     replicas,
		connections:  NewReplicaRegistry(name),
		data:         make(map[string]*Feature),
		rTree:        &rTree,
		vclock:       make(map[string]uint64),
		commands:     make(chan Command),
		ctx:          ctx,
		snapshotFile: snapshotFile,
		walFile:      walFile,
	}
}

func (e *Engine) Start() {
	_ = e.loadSnapshot()
	e.restoreRTree()

	wal, _ := e.loadWAL()
	e.applyWAL(wal)

	e.connectToReplicas()
	e.broadcastAllData()

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
	tx := &Transaction{
		Action:  action,
		Name:    e.name,
		Lsn:     e.vclock[e.name] + 1,
		Feature: feature,
	}
	return e.ApplyTransactionRaw(tx)
}

func (e *Engine) ApplyTransactionRaw(tx *Transaction) error {
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
	result := make(map[string]*geojson.Feature, len(e.data))
	for _, feature := range e.data {
		result[feature.Feature.ID.(string)] = feature.Feature
	}
	return result
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
		result[ID] = e.data[ID].Feature
	}

	return result
}

func (e *Engine) applyTransactionAndSave(tx *Transaction) error {
	applied, err := e.applyTransaction(tx)
	if err != nil || !applied {
		return err
	}
	if err := e.saveTransactionToWAL(tx); err != nil {
		return err
	}
	e.connections.Broadcast(tx)
	return nil
}

func (e *Engine) applyTransaction(tx *Transaction) (bool, error) {
	if tx.Lsn <= e.vclock[tx.Name] {
		return false, nil // tx is already applied
	}
	e.vclock[tx.Name] = tx.Lsn

	ID := tx.Feature.ID.(string)
	switch tx.Action {
	case Upsert:
		e.data[ID] = &Feature{tx.Name, tx.Lsn, tx.Feature}
		e.updateRTree(tx.Feature)
	case Delete:
		delete(e.data, ID)
		e.deleteFromRTree(tx.Feature)
	}
	return true, nil
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

// replication

func (e *Engine) connectToReplicas() {
	for _, replica := range e.replicas {
		u := url.URL{Scheme: "ws", Host: "127.0.0.1:8080", Path: "/" + replica + "/replication", RawQuery: "name=" + e.name}
		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			slog.Error("Dial error to "+replica, err)
			continue
		}
		e.connections.Add(replica, conn)
	}
}

func (e *Engine) broadcastAllData() {
	txs := make([]*Transaction, 0)
	for _, feature := range e.data {
		txs = append(txs, &Transaction{Upsert, feature.Name, feature.LSN, feature.Feature})
	}

	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lsn < txs[j].Lsn
	})

	for _, tx := range txs {
		e.connections.Broadcast(tx)
	}
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
		_, _ = e.applyTransaction(&tx)
	}
}

func (e *Engine) restoreRTree() {
	for _, feature := range e.data {
		e.updateRTree(feature.Feature)
	}
}

// utils for save data

func (e *Engine) saveSnapshot() error {
	data, err := json.Marshal(e.data)
	if err != nil {
		slog.Error("Failed to marshal data for snapshot", err)
		return err
	}

	if _, err := os.Stat(e.snapshotFile); os.IsNotExist(err) {
		_ = os.MkdirAll(filepath.Dir(e.snapshotFile), os.ModePerm)
		_, _ = os.Create(e.snapshotFile)
	}

	if err = os.WriteFile(e.snapshotFile, data, 0666); err != nil {
		slog.Error("Failed to write data to snapshot", err)
		return err
	}

	return nil
}

func (e *Engine) saveTransactionToWAL(tx *Transaction) error {
	if _, err := os.Stat(e.walFile); os.IsNotExist(err) {
		_ = os.MkdirAll(filepath.Dir(e.walFile), os.ModePerm)
		_, _ = os.Create(e.walFile)
	}

	file, err := os.OpenFile(e.walFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open the WAL file", err)
		return err
	}
	defer file.Close()

	data, err := json.Marshal(tx)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to serialize the transaction %v", tx), err)
		return err
	}

	_, err = file.Write(append(data, '\n'))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to save the transaction to WAL %v", tx), err)
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
