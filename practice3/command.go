package main

import "github.com/paulmach/orb/geojson"

type Command interface {
	Execute(engine *Engine)
}

type GetAllCommand struct {
	response chan map[string]*geojson.Feature
}

func (cmd *GetAllCommand) Execute(engine *Engine) {
	cmd.response <- engine.getAllData()
}

type GetCommand struct {
	coordinates [4]float64
	response    chan map[string]*geojson.Feature
}

func (cmd *GetCommand) Execute(engine *Engine) {
	cmd.response <- engine.getData(cmd.coordinates)
}

type ExistsCommand struct {
	ID       string
	response chan bool
}

func (cmd *ExistsCommand) Execute(engine *Engine) {
	_, exists := engine.getAllData()[cmd.ID]
	cmd.response <- exists
}

type ApplyCommand struct {
	tx     *Transaction
	errors chan error
}

func (cmd *ApplyCommand) Execute(engine *Engine) {
	err := engine.applyTransactionAndSave(cmd.tx)
	cmd.errors <- err
}

type SnapshotCommand struct {
	errors chan error
}

func (cmd *SnapshotCommand) Execute(engine *Engine) {
	err := engine.makeSnapshot()
	cmd.errors <- err
}
