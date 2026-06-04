// Package bus is the engine's RabbitMQ transport for OXO. It declares the
// per-scan topic exchange and the agent's queue, consumes inbound messages
// (unwrapping the control envelope and decoding the inner proto to a dict),
// and publishes a handler's emits back (encoding the dict and wrapping it in a
// fresh envelope). The codec does the proto↔dict conversion; this package owns
// the AMQP wiring and the OXO routing conventions.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/burogurama/goxo/internal/codec"
)

// Config is everything the bus needs to connect and route.
type Config struct {
	URL      string   // AMQP URI of the broker
	Exchange string   // per-scan topic exchange
	Agent    string   // self name: appended to the emit chain (the pipeline identity)
	Service  string   // queue-name override; the queue is (Service or Agent)+"_queue"
	Inputs   []string // input selectors, each bound as selector+".#"
}

// queueName is the agent's queue: a named instance (Service set) gets its own
// queue, otherwise the queue is derived from the agent name. This mirrors OXO,
// where service_name overrides the queue but never the chain identity.
func queueName(cfg Config) string {
	name := cfg.Service
	if name == "" {
		name = cfg.Agent
	}
	return name + "_queue"
}

// Delivery is one inbound message decoded to a dict, plus the agents path it
// carried and the ack callbacks bound to the underlying AMQP message. Nack
// does not requeue.
type Delivery struct {
	Selector  string
	Data      map[string]any
	Chain     []string
	MessageID string
	Headers   map[string]any
	Ack       func() error
	Nack      func() error
}

// Bus holds the AMQP connection and channel and the codec used to translate
// messages.
type Bus struct {
	cfg   Config
	codec *codec.Codec
	log   *slog.Logger
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

// Connect dials the broker, opens a channel, and declares the exchange, the
// agent's queue, its input bindings, and a prefetch of one. A nil logger
// defaults to slog.Default.
func Connect(cfg Config, c *codec.Codec, log *slog.Logger) (*Bus, error) {
	if log == nil {
		log = slog.Default()
	}
	var (
		conn *amqp.Connection
		err  error
	)
	conn, err = amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("bus: dial: %w", err)
	}
	var ch *amqp.Channel
	ch, err = conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("bus: open channel: %w", err)
	}
	var b *Bus = &Bus{cfg: cfg, codec: c, log: log, conn: conn, ch: ch, queue: queueName(cfg)}
	if err := b.setup(); err != nil {
		_ = b.Close()
		return nil, err
	}
	return b, nil
}

// setup declares the exchange, queue, prefetch, and bindings. The exchange
// arguments match OXO so a goxo engine and an OXO agent can share one.
func (b *Bus) setup() error {
	args := amqp.Table{"x-max-length": int32(10000), "x-overflow": "reject-publish"}
	if err := b.ch.ExchangeDeclare(b.cfg.Exchange, "topic", false, false, false, false, args); err != nil {
		return fmt.Errorf("bus: declare exchange %s: %w", b.cfg.Exchange, err)
	}
	if _, err := b.ch.QueueDeclare(b.queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("bus: declare queue %s: %w", b.queue, err)
	}
	if err := b.ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("bus: set qos: %w", err)
	}
	for _, sel := range b.cfg.Inputs {
		key := sel + ".#"
		if err := b.ch.QueueBind(b.queue, key, b.cfg.Exchange, false, nil); err != nil {
			return fmt.Errorf("bus: bind %s to %s: %w", key, b.queue, err)
		}
	}
	return nil
}

// Consume subscribes and calls handle for each inbound message until ctx is
// done or the AMQP channel closes. Prefetch is one, so handle runs for one
// message at a time and the next arrives only after this one is acked.
// Messages whose envelope or inner proto fails to decode are nacked (dropped)
// without reaching handle.
func (b *Bus) Consume(ctx context.Context, handle func(Delivery)) error {
	var (
		deliveries <-chan amqp.Delivery
		err        error
	)
	deliveries, err = b.ch.Consume(b.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("bus: consume %s: %w", b.queue, err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				// TODO: recover instead of exiting. OXO's MQ mixin re-runs the
				// consumer on an unexpected channel close and retries publishes
				// with backoff; goxo exits on a broker blip (relying on a
				// container restart) and fails emits without retry. Add
				// reconnect-with-backoff here and a retry in Publish.
				return errors.New("bus: consume channel closed")
			}
			b.dispatch(d, handle)
		}
	}
}

// dispatch turns one AMQP delivery into a bus Delivery and hands it to handle.
func (b *Bus) dispatch(d amqp.Delivery, handle func(Delivery)) {
	var selector string = selectorFromRoutingKey(d.RoutingKey)
	var (
		agents []string
		inner  []byte
		err    error
	)
	agents, inner, err = unwrapControl(b.codec, d.Body)
	if err != nil {
		b.log.Warn("drop unparsable envelope", "routing_key", d.RoutingKey, "err", err)
		_ = d.Nack(false, false)
		return
	}
	var data map[string]any
	data, err = b.codec.Decode(selector, inner)
	if err != nil {
		b.log.Warn("drop undecodable message", "selector", selector, "err", err)
		_ = d.Nack(false, false)
		return
	}
	handle(Delivery{
		Selector:  selector,
		Data:      data,
		Chain:     agents,
		MessageID: messageIDFromRoutingKey(d.RoutingKey),
		Headers:   map[string]any(d.Headers),
		Ack:       func() error { return d.Ack(false) },
		Nack:      func() error { return d.Nack(false, false) },
	})
}

// Publish encodes data for the selector, wraps it in a control envelope that
// appends this agent to the inbound chain, and publishes it on a fresh routing
// key (selector plus a unique id).
func (b *Bus) Publish(ctx context.Context, chain []string, selector string, data map[string]any) error {
	var (
		routingKey string
		msg        amqp.Publishing
		err        error
	)
	routingKey, msg, err = b.buildPublishing(chain, selector, data)
	if err != nil {
		return err
	}
	if err := b.ch.PublishWithContext(ctx, b.cfg.Exchange, routingKey, false, false, msg); err != nil {
		return fmt.Errorf("bus: publish %s: %w", routingKey, err)
	}
	return nil
}

// buildPublishing assembles the routing key and AMQP message for an emit,
// separate from the network call so it can be tested without a broker.
func (b *Bus) buildPublishing(chain []string, selector string, data map[string]any) (string, amqp.Publishing, error) {
	var (
		inner []byte
		err   error
	)
	inner, err = b.codec.Encode(selector, data)
	if err != nil {
		return "", amqp.Publishing{}, fmt.Errorf("bus: encode %s: %w", selector, err)
	}
	agents := make([]string, 0, len(chain)+1)
	agents = append(agents, chain...)
	agents = append(agents, b.cfg.Agent)
	var body []byte
	body, err = wrapControl(b.codec, agents, inner)
	if err != nil {
		return "", amqp.Publishing{}, err
	}
	var id string
	id, err = newID()
	if err != nil {
		return "", amqp.Publishing{}, err
	}
	routingKey := selector + "." + id
	return routingKey, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: body}, nil
}

// Close shuts the channel and connection.
func (b *Bus) Close() error {
	var err error
	if b.ch != nil {
		if cerr := b.ch.Close(); cerr != nil {
			err = cerr
		}
	}
	if b.conn != nil {
		if cerr := b.conn.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
