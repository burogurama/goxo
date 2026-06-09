package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestParse_AllFields(t *testing.T) {
	var c Config
	var err error
	c, err = Parse([]byte(`
handler: [python, /app/handler.py]
fdset: /app/agent.fdset
timeout: 30s
pool: 4
cap: 8
shutdown_grace: 5s
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := Config{
		Handler:             []string{"python", "/app/handler.py"},
		Fdset:               "/app/agent.fdset",
		HandlerTimeout:      30 * time.Second,
		WorkerPoolSize:      4,
		WorkerQueueSize:     8,
		WorkerShutdownGrace: 5 * time.Second,
	}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("got %+v, want %+v", c, want)
	}
}

func TestParse_HandlerString(t *testing.T) {
	var c Config
	var err error
	c, err = Parse([]byte(`handler: python /app/handler.py`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !reflect.DeepEqual(c.Handler, []string{"python", "/app/handler.py"}) {
		t.Fatalf("got handler %v", c.Handler)
	}
}

func TestParse_EmptyIsDefaults(t *testing.T) {
	for _, data := range []string{"", "# just a comment\n"} {
		var c Config
		var err error
		c, err = Parse([]byte(data))
		if err != nil {
			t.Fatalf("parse %q: %v", data, err)
		}
		if !reflect.DeepEqual(c, Default()) {
			t.Fatalf("parse %q: got %+v, want defaults", data, c)
		}
	}
}

func TestParse_PartialKeepsDefaults(t *testing.T) {
	var c Config
	var err error
	c, err = Parse([]byte(`pool: 3`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.WorkerPoolSize != 3 || c.WorkerQueueSize != 1 || c.WorkerShutdownGrace != 10*time.Second || c.HandlerTimeout != 0 {
		t.Fatalf("got %+v", c)
	}
}

func TestParse_UnknownKey(t *testing.T) {
	if _, err := Parse([]byte(`worker_cap: 8`)); err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestParse_BadDuration(t *testing.T) {
	if _, err := Parse([]byte(`timeout: soon`)); err == nil {
		t.Fatal("expected error for malformed duration")
	}
}

func TestLoad(t *testing.T) {
	var path string = filepath.Join(t.TempDir(), "goxo.yaml")
	if err := os.WriteFile(path, []byte("pool: 2\ncap: 5\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var c Config
	var err error
	c, err = Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.WorkerPoolSize != 2 || c.WorkerQueueSize != 5 {
		t.Fatalf("got %+v", c)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	var err error
	_, err = Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}
