package main

import "net/http"

type Router struct {
	mux      *http.ServeMux
	nodes    [][]string
	frontDir string
}

func NewRouter(mux *http.ServeMux, nodes [][]string, frontDir string) *Router {
	r := &Router{mux, nodes, frontDir}
	r.initHandlers()
	return r
}

func (r *Router) Run() {}

func (r *Router) Stop() {}

func (r *Router) initHandlers() {
	r.mux.Handle("/", http.FileServer(http.Dir(r.frontDir)))

	storage := r.nodes[0][0]
	r.mux.Handle("/select", http.RedirectHandler("/"+storage+"/select", http.StatusTemporaryRedirect))
	r.mux.Handle("/insert", http.RedirectHandler("/"+storage+"/insert", http.StatusTemporaryRedirect))
	r.mux.Handle("/replace", http.RedirectHandler("/"+storage+"/replace", http.StatusTemporaryRedirect))
	r.mux.Handle("/delete", http.RedirectHandler("/"+storage+"/delete", http.StatusTemporaryRedirect))
}
