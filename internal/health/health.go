// Package health serves the OXO liveness endpoint: a single GET /status that
// answers "OK". The OXO orchestrator polls it and restarts the container if the
// port stops answering, so the engine keeps a server up for its whole lifetime.
// goxo runs one handler process per phase, so there is no separate handler
// liveness to report — a running engine is a healthy one.
package health

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server is a running healthcheck endpoint. Close stops it.
type Server struct {
	srv *http.Server
	ln  net.Listener
}

// Serve binds addr and answers GET /status with "OK" on a background goroutine.
// It returns once the listener is open, so a bind failure surfaces to the
// caller rather than the goroutine. A nil logger defaults to slog.Default.
func Serve(addr string, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	var (
		ln  net.Listener
		err error
	)
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("health: listen %s: %w", addr, err)
	}
	var mux *http.ServeMux = http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	var srv *http.Server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health: serve failed", "err", err)
		}
	}()
	return &Server{srv: srv, ln: ln}, nil
}

// Addr is the address the server is listening on, resolved to a concrete port
// when addr requested port 0.
func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

// Close stops the server and releases the port.
func (s *Server) Close() error {
	return s.srv.Close()
}
