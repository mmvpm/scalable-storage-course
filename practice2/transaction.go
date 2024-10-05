package main

import (
	"github.com/paulmach/orb/geojson"
)

type ActionType string

const (
	Upsert ActionType = "upsert"
	Delete ActionType = "delete"
)

type Transaction struct {
	Action  ActionType       `json:"action"`
	Name    string           `json:"name"`
	Lsn     uint64           `json:"lsn"`
	Feature *geojson.Feature `json:"feature"`
}
