package bus

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/burogurama/goxo/internal/codec"
)

var uuid4 *regexp.Regexp = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestSelectorFromRoutingKey(t *testing.T) {
	cases := map[string]string{
		"v3.asset.ip.3f2c": "v3.asset.ip",
		"v3.control.abcd":  "v3.control",
		"a.b":              "a",
		"nodot":            "nodot",
	}
	for in, want := range cases {
		if got := selectorFromRoutingKey(in); got != want {
			t.Errorf("selectorFromRoutingKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMessageIDFromRoutingKey(t *testing.T) {
	cases := map[string]string{
		"v3.asset.ip.3f2c": "3f2c",
		"a.b":              "b",
		"nodot":            "",
	}
	for in, want := range cases {
		if got := messageIDFromRoutingKey(in); got != want {
			t.Errorf("messageIDFromRoutingKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewIDIsUniqueUUID4(t *testing.T) {
	var (
		a, b string
		err  error
	)
	a, err = newID()
	if err != nil {
		t.Fatalf("newID: %v", err)
	}
	b, err = newID()
	if err != nil {
		t.Fatalf("newID: %v", err)
	}
	if !uuid4.MatchString(a) {
		t.Errorf("newID = %q, not a uuid4", a)
	}
	if a == b {
		t.Errorf("newID returned the same value twice: %q", a)
	}
}

func TestWrapUnwrapRoundTrip(t *testing.T) {
	var c *codec.Codec = loadCodec(t)
	var (
		inner []byte
		err   error
	)
	inner, err = c.Encode("v3.asset.ip", map[string]any{"host": "10.0.0.1", "version": float64(4)})
	if err != nil {
		t.Fatalf("encode inner: %v", err)
	}
	var body []byte
	body, err = wrapControl(c, []string{"agent/a", "agent/b"}, inner)
	if err != nil {
		t.Fatalf("wrapControl: %v", err)
	}
	var (
		agents   []string
		gotInner []byte
	)
	agents, gotInner, err = unwrapControl(c, body)
	if err != nil {
		t.Fatalf("unwrapControl: %v", err)
	}
	if !reflect.DeepEqual(agents, []string{"agent/a", "agent/b"}) {
		t.Errorf("agents = %v, want [agent/a agent/b]", agents)
	}
	var out map[string]any
	out, err = c.Decode("v3.asset.ip", gotInner)
	if err != nil {
		t.Fatalf("decode inner: %v", err)
	}
	if !reflect.DeepEqual(out, map[string]any{"host": "10.0.0.1", "version": float64(4)}) {
		t.Errorf("inner round trip mismatch: %#v", out)
	}
}

func TestUnwrapToleratesEmptyEnvelope(t *testing.T) {
	var c *codec.Codec = loadCodec(t)
	// An envelope with no control and no message decodes to empty values.
	var (
		body []byte
		err  error
	)
	body, err = c.Encode("v3.control", map[string]any{})
	if err != nil {
		t.Fatalf("encode empty envelope: %v", err)
	}
	var (
		agents []string
		inner  []byte
	)
	agents, inner, err = unwrapControl(c, body)
	if err != nil {
		t.Fatalf("unwrapControl: %v", err)
	}
	if len(agents) != 0 {
		t.Errorf("agents = %v, want empty", agents)
	}
	if len(inner) != 0 {
		t.Errorf("inner = %v, want empty", inner)
	}
}
