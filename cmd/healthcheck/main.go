// Command healthcheck is the OXO container healthcheck probe. OXO's runtime
// injects `ostorlab agent healthcheck` as the Docker healthcheck for every
// agent; the goxo image symlinks ostorlab to this binary, which GETs the
// engine's /status endpoint and exits 0 when it answers 200, non-zero
// otherwise. It ignores its arguments — its only job is the probe — and reads
// the port from HEALTHCHECK_PORT (default 5000), the port the engine serves on.
package main

import (
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	os.Exit(probe(net.JoinHostPort("127.0.0.1", port())))
}

// port is the engine's healthcheck port: HEALTHCHECK_PORT, or 5000 by default.
func port() string {
	var p string = os.Getenv("HEALTHCHECK_PORT")
	if p == "" {
		return "5000"
	}
	return p
}

// probe GETs http://<addr>/status and returns 0 when it answers 200, 1
// otherwise — a non-200, a refused connection, or a timeout all mean unhealthy.
func probe(addr string) int {
	var client *http.Client = &http.Client{Timeout: 5 * time.Second}
	var (
		resp *http.Response
		err  error
	)
	resp, err = client.Get("http://" + addr + "/status")
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
