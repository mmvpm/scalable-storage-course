package main

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
)

type Router struct {
	mux      *http.ServeMux
	nodes    [][]string
	leaders  [][]string
	frontDir string
}

func NewRouter(mux *http.ServeMux, nodes [][]string, leaders [][]string, frontDir string) *Router {
	return &Router{mux, nodes, leaders, frontDir}
}

func (r *Router) Run() {
	r.initHandlers()
}

func (r *Router) Stop() {}

func (r *Router) initHandlers() {
	r.mux.Handle("/", http.FileServer(http.Dir(r.frontDir)))

	// any replica can return the data
	r.mux.HandleFunc("/select", func(w http.ResponseWriter, req *http.Request) {
		r.redirectWithQuery(w, req, "/"+r.chooseReplica()+"/select")
	})

	// only leader can modify the data
	r.mux.Handle("/insert", http.RedirectHandler("/"+r.chooseLeader()+"/insert", http.StatusTemporaryRedirect))
	r.mux.Handle("/replace", http.RedirectHandler("/"+r.chooseLeader()+"/replace", http.StatusTemporaryRedirect))
	r.mux.Handle("/delete", http.RedirectHandler("/"+r.chooseLeader()+"/delete", http.StatusTemporaryRedirect))

	// all replicas should make a snapshot
	r.mux.HandleFunc("/snapshot", r.snapshotHandler)
}

func (r *Router) redirectWithQuery(w http.ResponseWriter, req *http.Request, target string) {
	query := req.URL.RawQuery
	targetURL := &url.URL{Path: target, RawQuery: query}
	http.Redirect(w, req, targetURL.String(), http.StatusTemporaryRedirect)
}

func (r *Router) chooseLeader() string {
	return r.leaders[0][rand.IntN(len(r.leaders[0]))]
}

func (r *Router) chooseReplica() string {
	return r.nodes[0][rand.IntN(len(r.nodes[0]))]
}

func (r *Router) snapshotHandler(w http.ResponseWriter, req *http.Request) {
	for _, node := range r.nodes[0] {
		resp, err := http.Get(fmt.Sprintf("http://%s/%s/snapshot", req.Host, node))
		if err != nil {
			slog.Error("Failed to make snapshot on "+node, err)
			continue
		}
		_ = resp.Body.Close()
	}
	w.WriteHeader(http.StatusOK)
}
