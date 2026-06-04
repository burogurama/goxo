package settings

import (
	"path/filepath"
	"reflect"
	"testing"
)

// testdata/settings.binproto is a real AgentInstanceSettings serialized by OXO's
// AgentSettings.to_raw_proto: scalars plus six args (one per type tag), each arg
// value encoded the way the runtime writes it (raw bytes for "binary", JSON for
// the rest). It also sets orchestration-only fields (mem_limit, replicas,
// open_ports, caps, bus_management_url) the engine never reads, to prove
// unconsumed fields parse cleanly.

func loadFixture(t *testing.T) *Settings {
	t.Helper()
	var (
		s   *Settings
		err error
	)
	s, err = Load(filepath.Join("testdata", "settings.binproto"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return s
}

func TestParseDecodesConsumedFields(t *testing.T) {
	var s *Settings = loadFixture(t)
	if s.Key != "agent/ostorlab/nmap" {
		t.Errorf("Key = %q", s.Key)
	}
	if s.BusURL != "amqp://guest:guest@mq_42:5672/" {
		t.Errorf("BusURL = %q", s.BusURL)
	}
	if s.BusExchangeTopic != "ostorlab_topic_42" {
		t.Errorf("BusExchangeTopic = %q", s.BusExchangeTopic)
	}
	if s.RedisURL != "redis://redis_42:6379" {
		t.Errorf("RedisURL = %q", s.RedisURL)
	}
	if s.HealthcheckHost != "0.0.0.0" {
		t.Errorf("HealthcheckHost = %q", s.HealthcheckHost)
	}
	if s.HealthcheckPort != 5000 {
		t.Errorf("HealthcheckPort = %d", s.HealthcheckPort)
	}
	if s.CyclicProcessingLimit != 5 {
		t.Errorf("CyclicProcessingLimit = %d", s.CyclicProcessingLimit)
	}
	if s.DepthProcessingLimit != 30 {
		t.Errorf("DepthProcessingLimit = %d", s.DepthProcessingLimit)
	}
	if s.ServiceName != "nmap_42" {
		t.Errorf("ServiceName = %q", s.ServiceName)
	}
	if !reflect.DeepEqual(s.AcceptedAgents, []string{"agent/ostorlab/inputselector"}) {
		t.Errorf("AcceptedAgents = %v", s.AcceptedAgents)
	}
	if !reflect.DeepEqual(s.InSelectors, []string{"v3.asset.ip", "v3.asset.domain_name"}) {
		t.Errorf("InSelectors = %v", s.InSelectors)
	}
}

func TestArgDecodeByType(t *testing.T) {
	var s *Settings = loadFixture(t)
	byName := make(map[string]Arg, len(s.Args))
	for _, a := range s.Args {
		byName[a.Name] = a
	}
	cases := map[string]any{
		"fast":    true,
		"ports":   "0-65535",
		"timeout": float64(120),
		"scripts": []any{"http-title", "ssl-cert"},
		"options": map[string]any{"udp": true, "rate": float64(1000)},
		"blob":    []byte{0x00, 0x01, 0x02, 0xff},
	}
	for name, want := range cases {
		arg, ok := byName[name]
		if !ok {
			t.Errorf("arg %q missing", name)
			continue
		}
		var (
			got any
			err error
		)
		got, err = arg.Decode()
		if err != nil {
			t.Errorf("Decode(%q): %v", name, err)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Decode(%q) = %#v, want %#v", name, got, want)
		}
	}
}

// TestBinaryArgIsVerbatim guards that a "binary" arg is handed back as its exact
// wire bytes, never JSON-parsed.
func TestBinaryArgIsVerbatim(t *testing.T) {
	a := Arg{Name: "blob", Type: "binary", Value: []byte{0xff, 0x00, 0x10}}
	var (
		got any
		err error
	)
	got, err = a.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got, []byte{0xff, 0x00, 0x10}) {
		t.Errorf("Decode = %#v", got)
	}
}

func TestNonBinaryArgInvalidJSONErrors(t *testing.T) {
	a := Arg{Name: "ports", Type: "string", Value: []byte("not json")}
	if _, err := a.Decode(); err == nil {
		t.Error("Decode accepted invalid JSON")
	}
}

func TestParseEmptyYieldsZeroValues(t *testing.T) {
	var (
		s   *Settings
		err error
	)
	s, err = Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil): %v", err)
	}
	if !reflect.DeepEqual(s, &Settings{}) {
		t.Errorf("Parse(nil) = %#v, want zero Settings", s)
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	if _, err := Parse([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Error("Parse accepted non-proto bytes")
	}
}

func TestLoadMissingFileErrors(t *testing.T) {
	if _, err := Load(filepath.Join("testdata", "does-not-exist.binproto")); err == nil {
		t.Error("Load accepted a missing file")
	}
}

func TestDefaultPathMatchesOXO(t *testing.T) {
	if DefaultPath != "/tmp/settings.binproto" {
		t.Errorf("DefaultPath = %q", DefaultPath)
	}
}
