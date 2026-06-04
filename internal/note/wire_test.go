package note

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := Deliver{
		Type:     TypeDeliver,
		ID:       1,
		Selector: "v3.asset.ip",
		Data:     map[string]any{"host": "10.0.0.1", "version": float64(4)},
		Meta:     Meta{MessageID: "m-1", Headers: map[string]any{"depth": float64(2)}},
	}
	if err := WriteFrame(&buf, in); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	var (
		body []byte
		err  error
	)
	body, err = ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	var out Deliver
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in=%#v\nout=%#v", in, out)
	}
}

func TestReaderEmitDone(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Emit{
		Type: TypeEmit, ID: 1, Selector: "v3.report.vuln",
		Data: map[string]any{"title": "x"},
	}); err != nil {
		t.Fatalf("write emit: %v", err)
	}
	if err := WriteFrame(&buf, Done{Type: TypeDone, ID: 1, Status: StatusOK}); err != nil {
		t.Fatalf("write done: %v", err)
	}

	var r *Reader = NewReader(&buf)

	var (
		n1  HandlerNote
		err error
	)
	n1, err = r.Read()
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if n1.Emit == nil || n1.Done != nil {
		t.Fatalf("expected emit, got %#v", n1)
	}
	if n1.Emit.Selector != "v3.report.vuln" {
		t.Fatalf("emit selector = %q", n1.Emit.Selector)
	}

	var n2 HandlerNote
	n2, err = r.Read()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if n2.Done == nil || n2.Emit != nil {
		t.Fatalf("expected done, got %#v", n2)
	}
	if n2.Done.Status != StatusOK {
		t.Fatalf("done status = %q", n2.Done.Status)
	}

	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReaderRejectsEngineNote(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Init{Type: TypeInit, Protocol: 1}); err != nil {
		t.Fatalf("write init: %v", err)
	}

	var r *Reader = NewReader(&buf)
	var err error
	_, err = r.Read()
	if err == nil {
		t.Fatal("expected error for engine note")
	}
	if !strings.Contains(err.Error(), "unexpected handler note type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadFrameTooLarge(t *testing.T) {
	hdr := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	var err error
	_, err = ReadFrame(bytes.NewReader(hdr))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}
