package main

import "github.com/paulmach/orb/geojson"

type Feature struct {
	Name    string
	LSN     uint64
	Feature *geojson.Feature
}
