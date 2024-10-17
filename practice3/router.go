package main

import (
	"net/http"
	"net/url"
)

type Router struct {
	mux      *http.ServeMux
	nodes    [][]string
	frontDir string
}

func NewRouter(mux *http.ServeMux, nodes [][]string, frontDir string) *Router {
	return &Router{mux, nodes, frontDir}
}

func (r *Router) Run() {
	r.initHandlers()
}

func (r *Router) Stop() {}

func (r *Router) initHandlers() {
	r.mux.Handle("/", http.FileServer(http.Dir(r.frontDir)))

	storage := r.nodes[0][0]
	r.mux.HandleFunc("/select", func(w http.ResponseWriter, req *http.Request) {
		r.redirectWithQuery(w, req, "/"+storage+"/select")
	})
	r.mux.Handle("/insert", http.RedirectHandler("/"+storage+"/insert", http.StatusTemporaryRedirect))
	r.mux.Handle("/replace", http.RedirectHandler("/"+storage+"/replace", http.StatusTemporaryRedirect))
	r.mux.Handle("/delete", http.RedirectHandler("/"+storage+"/delete", http.StatusTemporaryRedirect))
	r.mux.Handle("/snapshot", http.RedirectHandler("/"+storage+"/snapshot", http.StatusTemporaryRedirect))
}

func (r *Router) redirectWithQuery(w http.ResponseWriter, req *http.Request, target string) {
	query := req.URL.RawQuery
	targetURL := &url.URL{Path: target, RawQuery: query}
	http.Redirect(w, req, targetURL.String(), http.StatusTemporaryRedirect)
}
