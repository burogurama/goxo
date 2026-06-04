package health

import (
	"io"
	"net/http"
	"testing"
)

func TestServeStatusOK(t *testing.T) {
	var (
		s   *Server
		err error
	)
	s, err = Serve("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer func() { _ = s.Close() }()

	var resp *http.Response
	resp, err = http.Get("http://" + s.Addr() + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "OK" {
		t.Fatalf("body = %q, want OK", body)
	}
}

func TestServeOtherPath404(t *testing.T) {
	var (
		s   *Server
		err error
	)
	s, err = Serve("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer func() { _ = s.Close() }()

	var resp *http.Response
	resp, err = http.Get("http://" + s.Addr() + "/other")
	if err != nil {
		t.Fatalf("GET /other: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestServeStatusRejectsNonGet(t *testing.T) {
	var (
		s   *Server
		err error
	)
	s, err = Serve("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer func() { _ = s.Close() }()

	var resp *http.Response
	resp, err = http.Post("http://"+s.Addr()+"/status", "text/plain", nil)
	if err != nil {
		t.Fatalf("POST /status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestCloseStopsServer(t *testing.T) {
	var (
		s   *Server
		err error
	)
	s, err = Serve("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	var addr string = s.Addr()
	if err = s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err = http.Get("http://" + addr + "/status"); err == nil {
		t.Fatal("expected error connecting after Close")
	}
}

func TestServeBadAddrErrors(t *testing.T) {
	if _, err := Serve("not-an-addr", nil); err == nil {
		t.Fatal("expected error for bad address")
	}
}
