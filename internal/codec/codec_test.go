package codec

import (
	"encoding/base64"
	"os"
	"reflect"
	"testing"
)

// testdata/oxo.fdset is regenerated from the checked-in real OXO protos with:
//
//	protoc -I testdata/proto --include_imports \
//	  --descriptor_set_out testdata/oxo.fdset \
//	  ostorlab/agent/message/proto/v3/asset/ip/ip.proto \
//	  ostorlab/agent/message/proto/v3/capture/http/request/request.proto \
//	  ostorlab/agent/message/proto/v3/control/control.proto \
//	  ostorlab/agent/message/proto/v3/report/cve/cve.proto \
//	  ostorlab/agent/message/proto/v3/capture/filesystem/filesystem.proto
func loadCodec(t *testing.T) *Codec {
	t.Helper()
	var (
		fdset []byte
		err   error
	)
	fdset, err = os.ReadFile("testdata/oxo.fdset")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var reg *Registry
	reg, err = NewRegistry(fdset)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	return New(reg)
}

func roundTrip(t *testing.T, c *Codec, selector string, in map[string]any) {
	t.Helper()
	var (
		wire []byte
		err  error
	)
	wire, err = c.Encode(selector, in)
	if err != nil {
		t.Fatalf("Encode(%s): %v", selector, err)
	}
	var out map[string]any
	out, err = c.Decode(selector, wire)
	if err != nil {
		t.Fatalf("Decode(%s): %v", selector, err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch for %s:\n in=%#v\nout=%#v", selector, in, out)
	}
}

func TestScalarRoundTrip(t *testing.T) {
	var c *Codec = loadCodec(t)
	// v3.asset.ip: string host, string mask, int32 version.
	in := map[string]any{
		"host":    "10.0.0.1",
		"mask":    "24",
		"version": float64(4), // int32 stays a JSON number
	}
	roundTrip(t, c, "v3.asset.ip", in)
}

func TestBytesAndNestedRepeated(t *testing.T) {
	var c *Codec = loadCodec(t)
	var b64 func([]byte) string = base64.StdEncoding.EncodeToString
	// v3.capture.http.request: bytes fields, uint32 port, repeated nested
	// `header` messages.
	in := map[string]any{
		"id":           "req-1",
		"method":       "GET",
		"host":         "example.com",
		"port":         float64(443),
		"content":      b64([]byte("GET / HTTP/1.1\r\n")),
		"http_version": b64([]byte("HTTP/1.1")),
		"headers": []any{
			map[string]any{"name": b64([]byte("Host")), "value": b64([]byte("example.com"))},
			map[string]any{"name": b64([]byte("Accept")), "value": b64([]byte("*/*"))},
		},
	}
	roundTrip(t, c, "v3.capture.http.request", in)
}

func TestCanonicalShape(t *testing.T) {
	var c *Codec = loadCodec(t)
	var (
		wire []byte
		err  error
	)
	wire, err = c.Encode("v3.capture.http.request", map[string]any{
		"content":      base64.StdEncoding.EncodeToString([]byte("hello")),
		"port":         float64(8080),
		"http_version": base64.StdEncoding.EncodeToString([]byte("HTTP/1.1")),
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out map[string]any
	out, err = c.Decode("v3.capture.http.request", wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	// bytes decode to a base64 string, not raw bytes.
	content, ok := out["content"].(string)
	if !ok {
		t.Fatalf("content should be a base64 string, got %T", out["content"])
	}
	if got, _ := base64.StdEncoding.DecodeString(content); string(got) != "hello" {
		t.Fatalf("content base64 mismatch: %q", content)
	}
	// uint32 decodes to a JSON number (float64 in Go), not a string.
	if _, ok := out["port"].(float64); !ok {
		t.Fatalf("port should be a number, got %T", out["port"])
	}
	// keys are the proto snake_case names, not camelCase.
	if _, ok := out["http_version"]; !ok {
		t.Fatalf("expected snake_case key http_version, got keys %v", keys(out))
	}
	if _, ok := out["httpVersion"]; ok {
		t.Fatalf("did not expect camelCase key httpVersion, got keys %v", keys(out))
	}
	// unset fields are omitted, not emitted as zero values.
	if _, ok := out["method"]; ok {
		t.Fatalf("unset field method should be absent, got keys %v", keys(out))
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestUint64IsString(t *testing.T) {
	var c *Codec = loadCodec(t)
	// v3.report.cve: uint64 published_date, int32 cwe.
	in := map[string]any{
		"cve_id":         "CVE-2021-44228",
		"cwe":            float64(502),
		"published_date": "1639094400", // 64-bit int on the wire, string on the boundary
	}
	var (
		wire []byte
		err  error
	)
	wire, err = c.Encode("v3.report.cve", in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out map[string]any
	out, err = c.Decode("v3.report.cve", wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if _, ok := out["published_date"].(string); !ok {
		t.Fatalf("uint64 published_date should be a string, got %T", out["published_date"])
	}
	if _, ok := out["cwe"].(float64); !ok {
		t.Fatalf("int32 cwe should be a number, got %T", out["cwe"])
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in=%#v\nout=%#v", in, out)
	}
}

func TestEnumIsValueName(t *testing.T) {
	var c *Codec = loadCodec(t)
	// v3.capture.filesystem: Event enum, decoded as its value name.
	in := map[string]any{
		"event":    "OPEN",
		"filename": "/etc/passwd",
		"pid":      float64(1234),
	}
	var (
		wire []byte
		err  error
	)
	wire, err = c.Encode("v3.capture.filesystem", in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out map[string]any
	out, err = c.Decode("v3.capture.filesystem", wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got, ok := out["event"].(string); !ok || got != "OPEN" {
		t.Fatalf("enum event should decode to its name %q, got %#v", "OPEN", out["event"])
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in=%#v\nout=%#v", in, out)
	}
}

// The control envelope is just another selector: v3.control resolves to
// ostorlab.agent.message.proto.v3.control.Message and round-trips like any
// message. The bus layer uses this to wrap/unwrap; nothing special is needed
// in the codec.
func TestControlEnvelopeIsJustASelector(t *testing.T) {
	var c *Codec = loadCodec(t)
	in := map[string]any{
		"control": map[string]any{
			"agents": []any{"agent/a", "agent/b"},
		},
		"message": base64.StdEncoding.EncodeToString([]byte("inner-payload-bytes")),
	}
	roundTrip(t, c, "v3.control", in)
}

func TestUnknownSelector(t *testing.T) {
	var c *Codec = loadCodec(t)
	if c.reg.Has("v3.no.such.thing") {
		t.Fatal("Has should be false for an unknown selector")
	}
	if _, err := c.Decode("v3.no.such.thing", []byte{}); err == nil {
		t.Fatal("Decode should error on an unknown selector")
	}
	if _, err := c.Encode("v3.no.such.thing", map[string]any{}); err == nil {
		t.Fatal("Encode should error on an unknown selector")
	}
}
