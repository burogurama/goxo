// Wire framing and the typed Reader and Writer for notes. Each note is a
// 4-byte big-endian length prefix followed by its JSON body.
package note

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxFrameSize caps a single note's JSON body. Frames larger than this are
// refused rather than allocated, so a corrupt or hostile length prefix can't
// drive an unbounded allocation. It must stay below 4 GiB so a body length
// always fits the 4-byte prefix.
const MaxFrameSize = 64 << 20

// ErrFrameTooLarge is returned when a frame's declared length exceeds
// MaxFrameSize.
var ErrFrameTooLarge = fmt.Errorf("note: frame exceeds %d bytes", MaxFrameSize)

// WriteFrame marshals v to JSON and writes it as one frame: a 4-byte
// big-endian length prefix followed by the JSON body.
func WriteFrame(w io.Writer, v any) error {
	var (
		body []byte
		err  error
	)
	body, err = json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// ReadFrame reads one frame and returns its JSON body. It returns io.EOF only
// when the reader is cleanly at a frame boundary; a truncated frame yields
// io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	var n uint32 = binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// Writer sends engine→handler notes over a pipe or socket. It stamps each
// note's Type field so callers pass only the payload.
type Writer struct {
	w io.Writer
}

// NewWriter wraps w for sending engine→handler notes.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Init stamps and sends an init note.
func (w *Writer) Init(n Init) error {
	n.Type = TypeInit
	return WriteFrame(w.w, n)
}

// Deliver stamps and sends a deliver note.
func (w *Writer) Deliver(n Deliver) error {
	n.Type = TypeDeliver
	return WriteFrame(w.w, n)
}

// EmitAck stamps and sends an emit_ack note.
func (w *Writer) EmitAck(n EmitAck) error {
	n.Type = TypeEmitAck
	return WriteFrame(w.w, n)
}

// Shutdown stamps and sends a shutdown note.
func (w *Writer) Shutdown(n Shutdown) error {
	n.Type = TypeShutdown
	return WriteFrame(w.w, n)
}

// HandlerNote is one decoded handler→engine note: exactly one of Emit, Done or
// Pickup is non-nil.
type HandlerNote struct {
	Emit   *Emit
	Done   *Done
	Pickup *Pickup
}

// Reader decodes handler→engine notes. Only emit, done and pickup are valid in
// that direction; any other type is an error.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps r for reading handler→engine notes.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// Read returns the next handler note. It returns io.EOF when the handler
// closes its end at a frame boundary.
func (r *Reader) Read() (HandlerNote, error) {
	var (
		body []byte
		err  error
	)
	body, err = ReadFrame(r.r)
	if err != nil {
		return HandlerNote{}, err
	}
	var peek struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &peek); err != nil {
		return HandlerNote{}, fmt.Errorf("note: decode type: %w", err)
	}
	switch peek.Type {
	case TypeEmit:
		var e Emit
		if err := json.Unmarshal(body, &e); err != nil {
			return HandlerNote{}, fmt.Errorf("note: decode emit: %w", err)
		}
		return HandlerNote{Emit: &e}, nil
	case TypeDone:
		var d Done
		if err := json.Unmarshal(body, &d); err != nil {
			return HandlerNote{}, fmt.Errorf("note: decode done: %w", err)
		}
		return HandlerNote{Done: &d}, nil
	case TypePickup:
		var pk Pickup
		if err := json.Unmarshal(body, &pk); err != nil {
			return HandlerNote{}, fmt.Errorf("note: decode pickup: %w", err)
		}
		return HandlerNote{Pickup: &pk}, nil
	default:
		return HandlerNote{}, fmt.Errorf("note: unexpected handler note type %q", peek.Type)
	}
}
