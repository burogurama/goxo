package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// addrOf strips the scheme from a test server URL, leaving host:port.
func addrOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestProbeHealthy(t *testing.T) {
	var srv *httptest.Server = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("OK"))
		}))
	defer srv.Close()

	if code := probe(addrOf(srv)); code != 0 {
		t.Fatalf("probe = %d, want 0", code)
	}
}

func TestProbeNon200(t *testing.T) {
	var srv *httptest.Server = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
	defer srv.Close()

	if code := probe(addrOf(srv)); code != 1 {
		t.Fatalf("probe = %d, want 1", code)
	}
}

func TestProbeNoServer(t *testing.T) {
	var srv *httptest.Server = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {}))
	var addr string = addrOf(srv)
	srv.Close()

	if code := probe(addr); code != 1 {
		t.Fatalf("probe = %d, want 1", code)
	}
}
