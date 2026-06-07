package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/burogurama/goxo/internal/note"
)

// The test binary doubles as a fake handler: when GOXO_FAKE is set it speaks the
// note protocol on stdin/stdout and never runs the suite. It reads init once,
// then loops over deliver notes, behaving per the mode, until stdin closes (EOF).
func TestMain(m *testing.M) {
	if mode := os.Getenv("GOXO_FAKE"); mode != "" {
		os.Exit(fakeHandler(mode))
	}
	os.Exit(m.Run())
}

func fakeHandler(mode string) int {
	if _, err := note.ReadFrame(os.Stdin); err != nil { // init
		return 2
	}
	for {
		var (
			body []byte
			err  error
		)
		body, err = note.ReadFrame(os.Stdin)
		if err != nil {
			return 0 // EOF: the engine closed stdin; exit cleanly
		}
		var peek struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(body, &peek); err != nil {
			return 2
		}
		var id int64
		switch peek.Type {
		case note.TypeDeliver:
			var d note.Deliver
			if err := json.Unmarshal(body, &d); err != nil {
				return 2
			}
			id = d.ID
		default:
			continue // emit_ack, shutdown: ignored by the fake
		}
		if code, exit := handleMsg(mode, id); exit {
			return code
		}
	}
}

// handleMsg processes one message in the fake handler. It returns (code, true)
// when the process should exit (crash modes), else (0, false).
func handleMsg(mode string, id int64) (int, bool) {
	switch mode {
	case "ok":
		writeHandler(note.Done{Type: note.TypeDone, ID: id, Status: note.StatusOK})
	case "slow":
		time.Sleep(300 * time.Millisecond)
		writeHandler(note.Done{Type: note.TypeDone, ID: id, Status: note.StatusOK})
	case "emit":
		writeHandler(note.Emit{Type: note.TypeEmit, ID: 1, Deliver: id,
			Selector: "v3.report.vuln", Data: map[string]any{"title": "x"}})
		writeHandler(note.Done{Type: note.TypeDone, ID: id, Status: note.StatusOK})
	case "bademit":
		writeHandler(note.Emit{Type: note.TypeEmit, ID: 1, Deliver: id,
			Selector: "v3.not.declared", Data: map[string]any{"x": float64(1)}})
		writeHandler(note.Done{Type: note.TypeDone, ID: id, Status: note.StatusOK})
	case "error":
		writeHandler(note.Done{Type: note.TypeDone, ID: id, Status: note.StatusError, Error: "boom"})
	case "crash":
		return 1, true
	case "crashonce":
		flag := os.Getenv("GOXO_FAKE_FLAG")
		if _, err := os.Stat(flag); err != nil {
			_, _ = os.Create(flag)
			return 1, true // first process crashes mid-message
		}
		writeHandler(note.Done{Type: note.TypeDone, ID: id, Status: note.StatusOK})
	case "hang":
		select {} // never reads stdin again; only SIGKILL stops it
	}
	return 0, false
}

func writeHandler(v any) {
	_ = note.WriteFrame(os.Stdout, v)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakePublisher struct {
	mu   sync.Mutex
	err  error
	msgs []published
}

type published struct {
	chain    []string
	selector string
	data     map[string]any
}

func (p *fakePublisher) Publish(chain []string, selector string, data map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, published{chain, selector, data})
	return p.err
}

func (p *fakePublisher) all() []published {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]published, len(p.msgs))
	copy(out, p.msgs)
	return out
}

// outcome captures a delivery's settlement, which now happens asynchronously off
// the worker's reader goroutine.
type outcome struct {
	done chan string
}

func newOutcome() *outcome { return &outcome{done: make(chan string, 1)} }

func (o *outcome) delivery() Delivery {
	return Delivery{
		Selector: "v3.asset.ip",
		Data:     map[string]any{"host": "10.0.0.1"},
		Chain:    []string{"agent/upstream"},
		Meta:     note.Meta{MessageID: "m-1"},
		Ack:      func() { o.done <- "ack" },
		Nack:     func() { o.done <- "nack" },
	}
}

func (o *outcome) wait(t *testing.T) string {
	t.Helper()
	select {
	case r := <-o.done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for settlement")
		return ""
	}
}

func newTestPool(t *testing.T, mode string, pub Publisher, size, capn int, timeout time.Duration, env ...string) *Pool {
	t.Helper()
	cfg := Config{
		Command:  []string{os.Args[0]},
		Env:      append([]string{"GOXO_FAKE=" + mode}, env...),
		Identity: note.Identity{Agent: "agent/test", Key: "test"},
		Outputs:  []string{"v3.report.vuln"},
		Protocol: 2,
		Timeout:  timeout,
	}
	var p *Pool = NewPool(cfg, pub, size, capn, testLogger())
	p.Start(context.Background())
	t.Cleanup(func() { p.Shutdown(2 * time.Second) })
	return p
}

func TestDispatch_OK(t *testing.T) {
	var p *Pool = newTestPool(t, "ok", &fakePublisher{}, 1, 1, 0)
	o := newOutcome()
	if !p.Dispatch(context.Background(), o.delivery()) {
		t.Fatal("delivery not accepted")
	}
	if r := o.wait(t); r != "ack" {
		t.Fatalf("expected ack, got %s", r)
	}
}

func TestDispatch_Emit(t *testing.T) {
	pub := &fakePublisher{}
	var p *Pool = newTestPool(t, "emit", pub, 1, 1, 0)
	o := newOutcome()
	p.Dispatch(context.Background(), o.delivery())
	if r := o.wait(t); r != "ack" {
		t.Fatalf("expected ack, got %s", r)
	}
	var msgs []published = pub.all()
	if len(msgs) != 1 || msgs[0].selector != "v3.report.vuln" {
		t.Fatalf("expected one publish on v3.report.vuln, got %#v", msgs)
	}
	// The emit must carry the delivery's inbound chain so the bus extends it.
	if len(msgs[0].chain) != 1 || msgs[0].chain[0] != "agent/upstream" {
		t.Fatalf("emit chain not relayed from delivery: %#v", msgs[0].chain)
	}
}

func TestDispatch_Error(t *testing.T) {
	var p *Pool = newTestPool(t, "error", &fakePublisher{}, 1, 1, 0)
	o := newOutcome()
	p.Dispatch(context.Background(), o.delivery())
	if r := o.wait(t); r != "nack" {
		t.Fatalf("expected nack, got %s", r)
	}
}

func TestDispatch_BadEmitIsLenient(t *testing.T) {
	pub := &fakePublisher{}
	var p *Pool = newTestPool(t, "bademit", pub, 1, 1, 0)
	o := newOutcome()
	p.Dispatch(context.Background(), o.delivery())
	if r := o.wait(t); r != "ack" {
		t.Fatalf("expected ack despite bad emit, got %s", r)
	}
	if len(pub.all()) != 0 {
		t.Fatalf("undeclared emit should not be published, got %#v", pub.all())
	}
}

func TestDispatch_CrashNacksInFlight(t *testing.T) {
	var p *Pool = newTestPool(t, "crash", &fakePublisher{}, 1, 1, 0)
	o := newOutcome()
	p.Dispatch(context.Background(), o.delivery())
	if r := o.wait(t); r != "nack" {
		t.Fatalf("expected nack on crash, got %s", r)
	}
}

func TestDispatch_RespawnAfterCrash(t *testing.T) {
	flag := filepath.Join(t.TempDir(), "crashed")
	var p *Pool = newTestPool(t, "crashonce", &fakePublisher{}, 1, 1, 0, "GOXO_FAKE_FLAG="+flag)

	// First message crashes the process and is nacked.
	o1 := newOutcome()
	p.Dispatch(context.Background(), o1.delivery())
	if r := o1.wait(t); r != "nack" {
		t.Fatalf("first message: expected nack on crash, got %s", r)
	}
	// The worker respawns; the next message is handled by the fresh process.
	o2 := newOutcome()
	p.Dispatch(context.Background(), o2.delivery())
	if r := o2.wait(t); r != "ack" {
		t.Fatalf("second message: expected ack after respawn, got %s", r)
	}
}

func TestDispatch_Timeout(t *testing.T) {
	var p *Pool = newTestPool(t, "hang", &fakePublisher{}, 1, 1, 200*time.Millisecond)
	o := newOutcome()
	var start time.Time = time.Now()
	p.Dispatch(context.Background(), o.delivery())
	if r := o.wait(t); r != "nack" {
		t.Fatalf("expected nack on timeout, got %s", r)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
}

func TestPool_ManyAcrossWorkers(t *testing.T) {
	var p *Pool = newTestPool(t, "ok", &fakePublisher{}, 4, 2, 0)
	const n = 50
	outs := make([]*outcome, n)
	for i := range outs {
		outs[i] = newOutcome()
		if !p.Dispatch(context.Background(), outs[i].delivery()) {
			t.Fatalf("dispatch %d not accepted", i)
		}
	}
	for i, o := range outs {
		if r := o.wait(t); r != "ack" {
			t.Fatalf("delivery %d: expected ack, got %s", i, r)
		}
	}
}

func TestDispatch_CtxCancelledReturnsFalse(t *testing.T) {
	// A pool whose process never comes up: Dispatch has no live worker, so a
	// cancelled context makes it give up without taking the delivery.
	cfg := Config{
		Command:  []string{os.Args[0]},
		Env:      []string{"GOXO_FAKE=ok"},
		Protocol: 2,
	}
	var p *Pool = NewPool(cfg, &fakePublisher{}, 1, 1, testLogger())
	// Not started: no process, so the only worker stays without w.cur.
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	o := newOutcome()
	if p.Dispatch(ctx, o.delivery()) {
		t.Fatal("expected Dispatch to refuse when ctx is cancelled and no worker is live")
	}
	select {
	case r := <-o.done:
		t.Fatalf("delivery must be untouched on refused dispatch, got %s", r)
	default:
	}
}

func TestShutdown_DrainsInFlight(t *testing.T) {
	pub := &fakePublisher{}
	var p *Pool = NewPool(Config{
		Command:  []string{os.Args[0]},
		Env:      []string{"GOXO_FAKE=slow"},
		Outputs:  []string{"v3.report.vuln"},
		Protocol: 2,
	}, pub, 1, 1, testLogger())
	p.Start(context.Background())

	o := newOutcome()
	if !p.Dispatch(context.Background(), o.delivery()) {
		t.Fatal("delivery not accepted")
	}
	// Shut down while the message is still being handled; a generous grace lets
	// the worker finish it before exiting.
	p.Shutdown(3 * time.Second)
	select {
	case r := <-o.done:
		if r != "ack" {
			t.Fatalf("expected ack after drain, got %s", r)
		}
	default:
		t.Fatal("in-flight message was not settled by shutdown drain")
	}
}
