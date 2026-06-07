package engine

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/burogurama/goxo/internal/bus"
	"github.com/burogurama/goxo/internal/manifest"
	"github.com/burogurama/goxo/internal/settings"
	"github.com/burogurama/goxo/internal/worker"
)

func jsonArg(t *testing.T, name string, v any) settings.Arg {
	t.Helper()
	var (
		b   []byte
		err error
	)
	b, err = json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal arg %q: %v", name, err)
	}
	return settings.Arg{Name: name, Type: "string", Value: b}
}

func sampleSettings(t *testing.T) *settings.Settings {
	t.Helper()
	return &settings.Settings{
		Key:                   "agent/ostorlab/nmap",
		BusURL:                "amqp://guest:guest@mq_42:5672/",
		BusExchangeTopic:      "ostorlab_topic_42",
		ServiceName:           "nmap_42",
		InSelectors:           []string{"v3.asset.ip", "v3.asset.domain_name"},
		CyclicProcessingLimit: 5,
		DepthProcessingLimit:  30,
		AcceptedAgents:        []string{"agent/ostorlab/inputselector"},
		Args: []settings.Arg{
			jsonArg(t, "ports", "0-65535"),
			jsonArg(t, "fast", true),
			{Name: "blob", Type: "binary", Value: []byte{0x00, 0x01, 0xff}},
		},
	}
}

func sampleParams() Params {
	return Params{
		Handler:   []string{"python", "handler.py"},
		FdsetPath: "/app/oxo.fdset",
		Universe:  "u-1",
		Timeout:   30 * time.Second,
		PoolSize:  2,
		WorkerCap: 3,
	}
}

func sampleManifest() *manifest.Manifest {
	return &manifest.Manifest{
		Name:         "agent/ostorlab/nmap",
		InSelectors:  []string{"v3.asset.ip.def", "v3.asset.domain_name.def"},
		OutSelectors: []string{"v3.report.vulnerability"},
		Args: []manifest.Arg{
			{Name: "ports", Value: "1-1024"}, // overridden by the settings value
			{Name: "timing", Value: "T3"},    // definition-only default
			{Name: "scripts", Value: nil},    // declared with no default
		},
	}
}

func TestBusConfig(t *testing.T) {
	var cfg bus.Config = busConfig(sampleManifest(), sampleSettings(t), sampleParams())
	if cfg.URL != "amqp://guest:guest@mq_42:5672/" {
		t.Errorf("URL = %q", cfg.URL)
	}
	if cfg.Exchange != "ostorlab_topic_42" {
		t.Errorf("Exchange = %q", cfg.Exchange)
	}
	// Agent is the chain identity; Service overrides the queue independently.
	if cfg.Agent != "agent/ostorlab/nmap" {
		t.Errorf("Agent = %q", cfg.Agent)
	}
	if cfg.Service != "nmap_42" {
		t.Errorf("Service = %q", cfg.Service)
	}
	if !reflect.DeepEqual(cfg.Inputs, []string{"v3.asset.ip", "v3.asset.domain_name"}) {
		t.Errorf("Inputs = %v", cfg.Inputs)
	}
	// Prefetch is the pool's total in-flight capacity: pool size times cap.
	if cfg.Prefetch != 6 {
		t.Errorf("Prefetch = %d, want 6", cfg.Prefetch)
	}
}

func TestWorkerConfig(t *testing.T) {
	var (
		rc  worker.Config
		err error
	)
	rc, err = workerConfig(sampleManifest(), sampleSettings(t), sampleParams())
	if err != nil {
		t.Fatalf("workerConfig: %v", err)
	}
	if !reflect.DeepEqual(rc.Command, []string{"python", "handler.py"}) {
		t.Errorf("Command = %v", rc.Command)
	}
	if rc.Identity.Key != "agent/ostorlab/nmap" || rc.Identity.Agent != "agent/ostorlab/nmap" {
		t.Errorf("Identity name/key = %+v", rc.Identity)
	}
	if rc.Identity.Universe != "u-1" {
		t.Errorf("Identity.Universe = %q", rc.Identity.Universe)
	}
	if rc.Protocol != Protocol {
		t.Errorf("Protocol = %d", rc.Protocol)
	}
	if rc.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v", rc.Timeout)
	}
	if !reflect.DeepEqual(rc.Outputs, []string{"v3.report.vulnerability"}) {
		t.Errorf("Outputs = %v", rc.Outputs)
	}
	// Config merges the definition defaults with the settings overrides by name.
	want := map[string]any{
		"ports":   "0-65535",                // settings override beats the definition default
		"timing":  "T3",                     // definition-only default
		"scripts": nil,                      // declared with no default, not overridden
		"fast":    true,                     // settings-only arg
		"blob":    []byte{0x00, 0x01, 0xff}, // settings-only arg
	}
	if !reflect.DeepEqual(rc.Config, want) {
		t.Errorf("Config = %#v, want %#v", rc.Config, want)
	}
}

func TestWorkerConfigNoArgsYieldsNilConfig(t *testing.T) {
	var s *settings.Settings = sampleSettings(t)
	s.Args = nil
	var m *manifest.Manifest = sampleManifest()
	m.Args = nil
	var (
		rc  worker.Config
		err error
	)
	rc, err = workerConfig(m, s, sampleParams())
	if err != nil {
		t.Fatalf("workerConfig: %v", err)
	}
	if rc.Config != nil {
		t.Errorf("Config = %#v, want nil for no args", rc.Config)
	}
}

func TestWorkerConfigBadArgErrors(t *testing.T) {
	var s *settings.Settings = sampleSettings(t)
	s.Args = []settings.Arg{{Name: "broken", Type: "number", Value: []byte("not-json")}}
	if _, err := workerConfig(sampleManifest(), s, sampleParams()); err == nil {
		t.Fatal("expected error for undecodable arg")
	}
}

func TestInSelectorsPrefersSettings(t *testing.T) {
	var s *settings.Settings = sampleSettings(t)
	var m *manifest.Manifest = sampleManifest()
	var got []string = inSelectors(s, m)
	if !reflect.DeepEqual(got, s.InSelectors) {
		t.Errorf("inSelectors = %v, want settings %v", got, s.InSelectors)
	}
	s.InSelectors = nil
	got = inSelectors(s, m)
	if !reflect.DeepEqual(got, m.InSelectors) {
		t.Errorf("inSelectors = %v, want manifest %v", got, m.InSelectors)
	}
}
