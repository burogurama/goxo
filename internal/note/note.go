// Package note defines the IPC protocol between the goxo engine and a
// handler process. The two exchange small JSON "notes" over the handler's
// stdin/stdout. Engine→handler notes are init, start, deliver, emit_ack and
// shutdown; handler→engine notes are emit and done.
//
// init is always the first note. done is always an explicit note, never an
// exit code. The deliver/done and emit/emit_ack notes carry an id that pairs
// a reply with its request.
package note

// Note types. Engine→handler: init, start, deliver, emit_ack, shutdown.
// Handler→engine: emit, done.
const (
	TypeInit     = "init"
	TypeStart    = "start"
	TypeDeliver  = "deliver"
	TypeEmitAck  = "emit_ack"
	TypeShutdown = "shutdown"
	TypeEmit     = "emit"
	TypeDone     = "done"
)

// Outcome statuses carried by emit_ack and done.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Identity names the agent instance the handler runs as. It mirrors the OXO
// agent identity (key, universe).
type Identity struct {
	Agent    string `json:"agent"`
	Key      string `json:"key"`
	Universe string `json:"universe,omitempty"`
}

// Meta carries per-message metadata that travels with a deliver.
type Meta struct {
	MessageID string         `json:"message_id,omitempty"`
	Headers   map[string]any `json:"headers,omitempty"`
}

// Init is the first note the engine sends. It hands the handler everything it
// needs for the run: protocol version, identity, agent config, and the
// declared input selectors.
type Init struct {
	Type     string         `json:"type"`
	Protocol int            `json:"protocol"`
	Identity Identity       `json:"identity"`
	Config   map[string]any `json:"config,omitempty"`
	Inputs   []string       `json:"inputs,omitempty"`
}

// Start runs the handler's start phase: its optional @OnStart lifecycle work,
// which any handler may implement whether or not it consumes messages. It
// carries no data; the handler emits and then sends done.
type Start struct {
	Type string `json:"type"`
}

// Deliver hands the handler one decoded scan-message to process. Data is the
// proto fields as a plain JSON-like dict; the engine owns the codec so the
// handler never sees protobuf.
type Deliver struct {
	Type     string         `json:"type"`
	ID       int64          `json:"id"`
	Selector string         `json:"selector"`
	Data     map[string]any `json:"data"`
	Meta     Meta           `json:"meta"`
}

// EmitAck answers a handler emit: ok if the engine published it, error (with a
// reason) if it rejected or failed it. It is advisory — done stays
// authoritative for the message outcome.
type EmitAck struct {
	Type   string `json:"type"`
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Shutdown asks a handler to stop within DeadlineMS.
type Shutdown struct {
	Type       string `json:"type"`
	DeadlineMS int64  `json:"deadline_ms,omitempty"`
}

// Emit is a handler's request to publish a message on one of its output
// selectors. The engine encodes Data and routes it on the bus.
type Emit struct {
	Type     string         `json:"type"`
	ID       int64          `json:"id"`
	Selector string         `json:"selector"`
	Data     map[string]any `json:"data"`
}

// Done ends the handler's work for an id: ok if it processed cleanly, error
// (with a reason) if it failed. The engine maps this to the bus ack/nack.
type Done struct {
	Type   string `json:"type"`
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
