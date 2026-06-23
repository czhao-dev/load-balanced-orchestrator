package api

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(h *Handlers) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/jobs", h.SubmitJob).Methods(http.MethodPost)
	r.HandleFunc("/jobs", h.ListJobs).Methods(http.MethodGet)
	r.HandleFunc("/jobs/{id}", h.GetJob).Methods(http.MethodGet)
	r.HandleFunc("/jobs/{id}", h.CancelJob).Methods(http.MethodDelete)
	r.HandleFunc("/healthz", Healthz).Methods(http.MethodGet)
	r.Handle("/metrics", promhttp.Handler()).Methods(http.MethodGet)
	return recoveryMiddleware(loggingMiddleware(r))
}

type Server struct {
	httpServer *http.Server
}

func NewServer(addr string, h *Handlers) *Server {
	return &Server{httpServer: &http.Server{Addr: addr, Handler: NewRouter(h)}}
}

// Start blocks until the server stops; it returns http.ErrServerClosed on
// a graceful Shutdown, which callers should treat as a normal exit.
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
