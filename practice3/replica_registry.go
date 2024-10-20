package main

import (
	"github.com/gorilla/websocket"
	"log/slog"
	"sync"
)

type ReplicaRegistry struct {
	name        string
	mu          sync.Mutex
	connections map[string]*websocket.Conn
}

func NewReplicaRegistry(name string) *ReplicaRegistry {
	return &ReplicaRegistry{
		name:        name,
		connections: make(map[string]*websocket.Conn),
	}
}

func (r *ReplicaRegistry) Add(name string, conn *websocket.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connections[name] = conn
}

func (r *ReplicaRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.connections, name)
}

func (r *ReplicaRegistry) Broadcast(tx *Transaction) {
	if tx.Name != r.name {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for replica, conn := range r.connections {
		if err := conn.WriteJSON(tx); err != nil {
			slog.Error("Error broadcasting to "+replica, err)
			go r.Remove(replica)
		}
	}
}
