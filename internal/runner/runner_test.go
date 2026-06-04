package runner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/burogurama/goxo/internal/note"
)

// The test binary doubles as a fake handler: when GOXO_FAKE is set it reads
// init then the phase note (deliver or start) from stdin, behaves per the mode,
// and exits — never running the test suite. A start note unmarshals into a
// zero-valued Deliver, so the same body serves both phases.
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
	var (
		db  []byte
		err error
	)
	db, err = note.ReadFrame(os.Stdin) // deliver or start
	if err != nil {
		return 2
	}
	var dlv note.Deliver
	if err := json.Unmarshal(db, &dlv); err != nil {
		return 2
	}

	switch mode {
	case "ok":
		writeHandler(note.Done{Type: note.TypeDone, ID: dlv.ID, Status: note.StatusOK})
	case "emit":
		writeHandler(note.Emit{Type: note.TypeEmit, ID: dlv.ID,
			Selector: "v3.report.vuln", Data: map[string]any{"title": "x"}})
		writeHandler(note.Done{Type: note.TypeDone, ID: dlv.ID, Status: note.StatusOK})
	case "bademit":
		writeHandler(note.Emit{Type: note.TypeEmit, ID: dlv.ID,
			Selector: "v3.not.declared", Data: map[string]any{"x": float64(1)}})
		writeHandler(note.Done{Type: note.TypeDone, ID: dlv.ID, Status: note.StatusOK})
	case "error":
		writeHandler(note.Done{Type: note.TypeDone, ID: dlv.ID,
			Status: note.StatusError, Error: "boom"})
	case "trailing":
		writeHandler(note.Done{Type: note.TypeDone, ID: dlv.ID, Status: note.StatusOK})
		// Output after done must be drained by the runner, never republished.
		writeHandler(note.Emit{Type: note.TypeEmit, ID: dlv.ID,
			Selector: "v3.report.vuln", Data: map[string]any{"late": true}})
	case "crash":
		return 1
	case "timeout":
		select {} // block until the runner's context kills us
	}
	return 0
}

func writeHandler(v any) {
	_ = note.WriteFrame(os.Stdout, v)
}

type fakePublisher struct {
	mu   sync.Mutex
	err  error
	msgs []published
}

type published struct {
	selector string
	data     map[string]any
}

func (p *fakePublisher) Publish(chain []string, selector string, data map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, published{selector, data})
	return p.err
}

func (p *fakePublisher) all() []published {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]published, len(p.msgs))
	copy(out, p.msgs)
	return out
}

func newRunner(mode string, pub Publisher, timeout time.Duration) *Runner {
	return New(Config{
		Command:  []string{os.Args[0]},
		Env:      []string{"GOXO_FAKE=" + mode},
		Identity: note.Identity{Agent: "agent/test", Key: "test"},
		Outputs:  []string{"v3.report.vuln"},
		Protocol: 1,
		Timeout:  timeout,
	}, pub, nil)
}

type outcome struct {
	acked  bool
	nacked bool
}

func (o *outcome) delivery() Delivery {
	return Delivery{
		Selector: "v3.asset.ip",
		Data:     map[string]any{"host": "10.0.0.1"},
		Meta:     note.Meta{MessageID: "m-1"},
		Ack:      func() { o.acked = true },
		Nack:     func() { o.nacked = true },
	}
}

func TestHandle_OK(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("ok", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if !o.acked || o.nacked {
		t.Fatalf("expected ack, got acked=%v nacked=%v", o.acked, o.nacked)
	}
}

func TestHandle_Emit(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("emit", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if !o.acked {
		t.Fatalf("expected ack")
	}
	var msgs []published = pub.all()
	if len(msgs) != 1 || msgs[0].selector != "v3.report.vuln" {
		t.Fatalf("expected one publish on v3.report.vuln, got %#v", msgs)
	}
}

func TestHandle_Error(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("error", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if o.acked || !o.nacked {
		t.Fatalf("expected nack, got acked=%v nacked=%v", o.acked, o.nacked)
	}
}

func TestHandle_Crash(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("crash", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if o.acked || !o.nacked {
		t.Fatalf("expected nack on crash, got acked=%v nacked=%v", o.acked, o.nacked)
	}
}

func TestHandle_BadEmitIsLenient(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("bademit", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if !o.acked {
		t.Fatalf("expected ack despite bad emit")
	}
	if len(pub.all()) != 0 {
		t.Fatalf("undeclared emit should not be published, got %#v", pub.all())
	}
}

func TestHandle_TrailingOutputAfterDoneIsDrained(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("trailing", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if !o.acked || o.nacked {
		t.Fatalf("expected ack, got acked=%v nacked=%v", o.acked, o.nacked)
	}
	if len(pub.all()) != 0 {
		t.Fatalf("output after done must not be published, got %#v", pub.all())
	}
}

func TestHandle_EmptyCommandNacks(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	New(Config{Timeout: time.Second}, pub, nil).Handle(context.Background(), o.delivery())
	if o.acked || !o.nacked {
		t.Fatalf("expected nack on empty command, got acked=%v nacked=%v", o.acked, o.nacked)
	}
}

func TestHandle_NonPositiveTimeoutNacks(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	newRunner("ok", pub, 0).Handle(context.Background(), o.delivery())
	if o.acked || !o.nacked {
		t.Fatalf("expected nack on non-positive timeout, got acked=%v nacked=%v", o.acked, o.nacked)
	}
}

func TestHandle_PublishErrorStillAcks(t *testing.T) {
	var o outcome
	pub := &fakePublisher{err: errors.New("bus down")}
	newRunner("emit", pub, 5*time.Second).Handle(context.Background(), o.delivery())
	if !o.acked || o.nacked {
		t.Fatalf("done is authoritative despite a publish error, got acked=%v nacked=%v", o.acked, o.nacked)
	}
	if len(pub.all()) != 1 {
		t.Fatalf("expected one publish attempt, got %#v", pub.all())
	}
}

func TestStart_OK(t *testing.T) {
	pub := &fakePublisher{}
	if err := newRunner("ok", pub, 5*time.Second).Start(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestStart_Emit(t *testing.T) {
	pub := &fakePublisher{}
	if err := newRunner("emit", pub, 5*time.Second).Start(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	var msgs []published = pub.all()
	if len(msgs) != 1 || msgs[0].selector != "v3.report.vuln" {
		t.Fatalf("expected one publish on v3.report.vuln, got %#v", msgs)
	}
}

func TestStart_Error(t *testing.T) {
	pub := &fakePublisher{}
	if err := newRunner("error", pub, 5*time.Second).Start(context.Background()); err == nil {
		t.Fatal("expected error when handler reports done error")
	}
}

func TestStart_CrashErrors(t *testing.T) {
	pub := &fakePublisher{}
	if err := newRunner("crash", pub, 5*time.Second).Start(context.Background()); err == nil {
		t.Fatal("expected error when handler exits before done")
	}
}

func TestStart_Timeout(t *testing.T) {
	pub := &fakePublisher{}
	var start time.Time = time.Now()
	if err := newRunner("timeout", pub, 200*time.Millisecond).Start(context.Background()); err == nil {
		t.Fatal("expected error on timeout")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
}

func TestSubmit_CtxCancelledReturnsErr(t *testing.T) {
	var p *Pool = NewPool(newRunner("ok", &fakePublisher{}, time.Second), 1)
	p.sem <- struct{}{} // fill the only slot so acquire must block
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	var o outcome
	if err := p.Submit(ctx, o.delivery()); err == nil {
		t.Fatalf("expected ctx error when no slot is free and ctx is cancelled")
	}
	if o.acked || o.nacked {
		t.Fatalf("delivery must be untouched on failed submit, got acked=%v nacked=%v", o.acked, o.nacked)
	}
	<-p.sem
}

func TestHandle_Timeout(t *testing.T) {
	var o outcome
	pub := &fakePublisher{}
	var start time.Time = time.Now()
	newRunner("timeout", pub, 200*time.Millisecond).Handle(context.Background(), o.delivery())
	if o.acked || !o.nacked {
		t.Fatalf("expected nack on timeout, got acked=%v nacked=%v", o.acked, o.nacked)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
}

func TestPool_AllHandled(t *testing.T) {
	pub := &fakePublisher{}
	var r *Runner = newRunner("ok", pub, 5*time.Second)
	var p *Pool = NewPool(r, 4)

	const n = 12
	outcomes := make([]*outcome, n)
	for i := range outcomes {
		outcomes[i] = &outcome{}
		if err := p.Submit(context.Background(), outcomes[i].delivery()); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	p.Wait()

	for i, o := range outcomes {
		if !o.acked || o.nacked {
			t.Fatalf("delivery %d: acked=%v nacked=%v", i, o.acked, o.nacked)
		}
	}
}
