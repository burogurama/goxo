package bus

import (
	"log/slog"
	"os"
	"reflect"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/burogurama/goxo/internal/codec"
)

func loadCodec(t *testing.T) *codec.Codec {
	t.Helper()
	var (
		fdset []byte
		err   error
	)
	fdset, err = os.ReadFile("testdata/oxo.fdset")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var reg *codec.Registry
	reg, err = codec.NewRegistry(fdset)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	return codec.New(reg)
}

func testBus(t *testing.T) *Bus {
	t.Helper()
	return &Bus{
		cfg:   Config{Exchange: "ostorlab_topic_1", Agent: "goxo"},
		codec: loadCodec(t),
		log:   slog.Default(),
	}
}

// ackCall and fakeAck record what dispatch asks of the AMQP message.
type ackCall struct {
	tag     uint64
	requeue bool
	kind    string
}

type fakeAck struct {
	calls []ackCall
}

func (f *fakeAck) Ack(tag uint64, multiple bool) error {
	f.calls = append(f.calls, ackCall{tag: tag, kind: "ack"})
	return nil
}

func (f *fakeAck) Nack(tag uint64, multiple, requeue bool) error {
	f.calls = append(f.calls, ackCall{tag: tag, requeue: requeue, kind: "nack"})
	return nil
}

func (f *fakeAck) Reject(tag uint64, requeue bool) error {
	f.calls = append(f.calls, ackCall{tag: tag, requeue: requeue, kind: "reject"})
	return nil
}

func TestQueueName(t *testing.T) {
	// Service overrides the queue; without it the queue derives from the agent.
	if got := queueName(Config{Agent: "agent/ostorlab/nmap", Service: "nmap_42"}); got != "nmap_42_queue" {
		t.Errorf("queueName with service = %q", got)
	}
	if got := queueName(Config{Agent: "agent/ostorlab/nmap"}); got != "agent/ostorlab/nmap_queue" {
		t.Errorf("queueName without service = %q", got)
	}
}

func TestBuildPublishingWrapsAndRoutes(t *testing.T) {
	var b *Bus = testBus(t)
	data := map[string]any{"host": "10.0.0.1", "version": float64(4)}
	var (
		routingKey string
		msg        amqp.Publishing
		err        error
	)
	routingKey, msg, err = b.buildPublishing([]string{"upstream"}, "v3.asset.ip", data)
	if err != nil {
		t.Fatalf("buildPublishing: %v", err)
	}
	if selectorFromRoutingKey(routingKey) != "v3.asset.ip" {
		t.Errorf("routing key %q has wrong selector", routingKey)
	}
	if !uuid4.MatchString(messageIDFromRoutingKey(routingKey)) {
		t.Errorf("routing key %q has no uuid suffix", routingKey)
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode = %d, want persistent", msg.DeliveryMode)
	}
	// The body wraps the inner message and appends this agent to the chain.
	var (
		agents []string
		inner  []byte
	)
	agents, inner, err = unwrapControl(b.codec, msg.Body)
	if err != nil {
		t.Fatalf("unwrap published body: %v", err)
	}
	if !reflect.DeepEqual(agents, []string{"upstream", "goxo"}) {
		t.Errorf("agents = %v, want [upstream goxo]", agents)
	}
	var out map[string]any
	out, err = b.codec.Decode("v3.asset.ip", inner)
	if err != nil {
		t.Fatalf("decode inner: %v", err)
	}
	if !reflect.DeepEqual(out, data) {
		t.Errorf("inner = %#v, want %#v", out, data)
	}
}

func TestBuildPublishingDoesNotMutateChain(t *testing.T) {
	var b *Bus = testBus(t)
	chain := []string{"upstream"}
	if _, _, err := b.buildPublishing(chain, "v3.asset.ip", map[string]any{"host": "x"}); err != nil {
		t.Fatalf("buildPublishing: %v", err)
	}
	if !reflect.DeepEqual(chain, []string{"upstream"}) {
		t.Errorf("caller chain was mutated to %v", chain)
	}
}

func TestDispatchDecodesAndAcks(t *testing.T) {
	var b *Bus = testBus(t)
	data := map[string]any{"host": "10.0.0.1", "version": float64(4)}
	body := wrap(t, b, []string{"sender"}, "v3.asset.ip", data)

	fake := &fakeAck{}
	d := amqp.Delivery{
		Acknowledger: fake,
		DeliveryTag:  7,
		RoutingKey:   "v3.asset.ip.3f2c",
		Body:         body,
	}
	var got Delivery
	var called bool
	b.dispatch(d, func(in Delivery) {
		got = in
		called = true
	})
	if !called {
		t.Fatal("handle was not called")
	}
	if got.Selector != "v3.asset.ip" {
		t.Errorf("selector = %q", got.Selector)
	}
	if got.MessageID != "3f2c" {
		t.Errorf("message id = %q", got.MessageID)
	}
	if !reflect.DeepEqual(got.Chain, []string{"sender"}) {
		t.Errorf("chain = %v", got.Chain)
	}
	if !reflect.DeepEqual(got.Data, data) {
		t.Errorf("data = %#v", got.Data)
	}
	if err := got.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != (ackCall{tag: 7, kind: "ack"}) {
		t.Errorf("ack calls = %+v", fake.calls)
	}
}

func TestDispatchNackDoesNotRequeue(t *testing.T) {
	var b *Bus = testBus(t)
	body := wrap(t, b, nil, "v3.asset.ip", map[string]any{"host": "x"})
	fake := &fakeAck{}
	d := amqp.Delivery{Acknowledger: fake, DeliveryTag: 9, RoutingKey: "v3.asset.ip.id", Body: body}
	b.dispatch(d, func(in Delivery) {
		if err := in.Nack(); err != nil {
			t.Fatalf("Nack: %v", err)
		}
	})
	if len(fake.calls) != 1 || fake.calls[0] != (ackCall{tag: 9, requeue: false, kind: "nack"}) {
		t.Errorf("nack calls = %+v, want one nack with requeue=false", fake.calls)
	}
}

func TestDispatchDropsUnparsableEnvelope(t *testing.T) {
	var b *Bus = testBus(t)
	fake := &fakeAck{}
	d := amqp.Delivery{Acknowledger: fake, DeliveryTag: 3, RoutingKey: "v3.asset.ip.id", Body: []byte{0xff, 0xff, 0xff}}
	var called bool
	b.dispatch(d, func(Delivery) { called = true })
	if called {
		t.Error("handle was called for a poison envelope")
	}
	if len(fake.calls) != 1 || fake.calls[0].kind != "nack" || fake.calls[0].requeue {
		t.Errorf("calls = %+v, want one nack with requeue=false", fake.calls)
	}
}

func TestDispatchDropsUndecodableInner(t *testing.T) {
	var b *Bus = testBus(t)
	// Valid envelope, but the inner bytes are not a valid v3.asset.ip.
	var (
		body []byte
		err  error
	)
	body, err = wrapControl(b.codec, []string{"sender"}, []byte{0xff, 0xff})
	if err != nil {
		t.Fatalf("wrapControl: %v", err)
	}
	fake := &fakeAck{}
	d := amqp.Delivery{Acknowledger: fake, DeliveryTag: 4, RoutingKey: "v3.asset.ip.id", Body: body}
	var called bool
	b.dispatch(d, func(Delivery) { called = true })
	if called {
		t.Error("handle was called for an undecodable inner message")
	}
	if len(fake.calls) != 1 || fake.calls[0].kind != "nack" {
		t.Errorf("calls = %+v, want one nack", fake.calls)
	}
}

// TestParseURIAcceptsUnderscoreHost guards the Docker-service-name case: OXO
// brokers run at hosts like mq_<scan_id>, which a strict URL parser rejects.
func TestParseURIAcceptsUnderscoreHost(t *testing.T) {
	var (
		u   amqp.URI
		err error
	)
	u, err = amqp.ParseURI("amqp://guest:guest@mq_42:5672/")
	if err != nil {
		t.Fatalf("ParseURI rejected underscore host: %v", err)
	}
	if u.Host != "mq_42" || u.Port != 5672 {
		t.Errorf("parsed host/port = %q:%d", u.Host, u.Port)
	}
}

func wrap(t *testing.T, b *Bus, chain []string, selector string, data map[string]any) []byte {
	t.Helper()
	var (
		inner []byte
		err   error
	)
	inner, err = b.codec.Encode(selector, data)
	if err != nil {
		t.Fatalf("encode inner: %v", err)
	}
	var body []byte
	body, err = wrapControl(b.codec, chain, inner)
	if err != nil {
		t.Fatalf("wrapControl: %v", err)
	}
	return body
}
