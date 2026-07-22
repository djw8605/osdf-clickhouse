// Package amqp implements a resilient RabbitMQ consumer for the fstream
// collector exchange. It uses the maintained github.com/rabbitmq/amqp091-go
// fork (not the deprecated streadway/amqp used upstream).
//
// Design:
//   - One durable queue per configured exchange, named "<prefix>.<exchange>",
//     shared by all pods so they act as competing consumers.
//   - Exchanges are declared passively by default so we never redeclare an
//     existing exchange with a conflicting type (which would kill the channel).
//   - Deliveries from every queue are fanned into a single stable output
//     channel that survives reconnects.
//   - Manual ack: the batcher acks only after ClickHouse commits.
//   - Reconnect with exponential backoff for connection loss.
//   - Authentication is plain username/password carried in the AMQP URL.
package amqp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/djw8605/osdf-clickhouse/ingester/internal/config"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/metrics"
)

// Delivery is a received message tagged with the exchange we bound it from,
// plus the acknowledgement handles. Ack/Nack forward to the underlying channel;
// after a reconnect the old channel is gone and these return an error, which
// the caller treats as "leave it to broker requeue + dedup".
type Delivery struct {
	Exchange string
	Body     []byte
	raw      amqp.Delivery
}

// Ack acknowledges the message.
func (d Delivery) Ack() error { return d.raw.Ack(false) }

// Nack negatively-acknowledges the message; requeue controls whether the broker
// redelivers it (false lets a configured dead-letter exchange capture it).
func (d Delivery) Nack(requeue bool) error { return d.raw.Nack(false, requeue) }

// Consumer manages the RabbitMQ connection lifecycle.
type Consumer struct {
	cfg     *config.Config
	log     *slog.Logger
	metrics *metrics.Metrics

	out chan Delivery
}

// New constructs a Consumer. Call Run to start it.
func New(cfg *config.Config, log *slog.Logger, m *metrics.Metrics) *Consumer {
	return &Consumer{
		cfg:     cfg,
		log:     log,
		metrics: m,
		// Buffer roughly one prefetch window so a brief batcher stall does not
		// immediately block the AMQP reader.
		out: make(chan Delivery, cfg.Prefetch),
	}
}

// Deliveries returns the stable output channel. It is closed when Run returns.
func (c *Consumer) Deliveries() <-chan Delivery { return c.out }

// Run connects and consumes until ctx is cancelled, reconnecting on failure.
func (c *Consumer) Run(ctx context.Context) {
	defer close(c.out)
	backoff := c.cfg.ReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.metrics.AMQPReconnects.Inc()
			c.log.Warn("amqp session ended, reconnecting", "error", err, "backoff", backoff.String())
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > c.cfg.ReconnectMax {
				backoff = c.cfg.ReconnectMax
			}
			continue
		}
		backoff = c.cfg.ReconnectMin
	}
}

// session runs one connection lifetime: dial, declare, consume, and block until
// the connection drops or ctx is cancelled.
func (c *Consumer) session(ctx context.Context) error {
	conn, err := amqp.DialConfig(c.cfg.AMQPURL, amqp.Config{
		Heartbeat: 10 * time.Second,
		Properties: amqp.Table{
			"connection_name": "osdf-clickhouse-ingester",
		},
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	for _, exchange := range c.cfg.Exchanges {
		if err := c.setupExchange(ch, exchange); err != nil {
			return fmt.Errorf("setup exchange %q: %w", exchange, err)
		}
	}

	c.log.Info("amqp connected and consuming",
		"exchanges", strings.Join(c.cfg.Exchanges, ","),
		"prefetch", c.cfg.Prefetch)

	// Fan-in: one goroutine per queue forwarding into c.out.
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, exchange := range c.cfg.Exchanges {
		deliveries, err := ch.Consume(
			c.cfg.QueueName(exchange), // queue
			"",                        // consumer tag (auto)
			false,                     // autoAck = false (manual ack)
			false,                     // exclusive
			false,                     // noLocal
			false,                     // noWait
			nil,                       // args
		)
		if err != nil {
			return fmt.Errorf("consume %q: %w", exchange, err)
		}
		wg.Add(1)
		go c.forward(sessionCtx, exchange, deliveries, &wg)
	}

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return nil
	case err := <-closeCh:
		cancel()
		wg.Wait()
		if err != nil {
			return fmt.Errorf("connection closed: %w", err)
		}
		return fmt.Errorf("connection closed")
	}
}

// setupExchange declares the queue and binds it to the exchange. The exchange
// itself is declared passively (or actively with the configured type) so we
// never conflict with the pre-provisioned broker exchange.
func (c *Consumer) setupExchange(ch *amqp.Channel, exchange string) error {
	if c.cfg.PassiveDeclare {
		if err := ch.ExchangeDeclarePassive(exchange, c.cfg.ExchangeType, true, false, false, false, nil); err != nil {
			// Passive declare fails if the exchange does not exist OR if the
			// type we pass disagrees with the server. It also kills the
			// channel, so surface a clear error for the reconnect loop.
			return fmt.Errorf("passive exchange declare (does it exist? is EXCHANGE_TYPE correct?): %w", err)
		}
	} else {
		if err := ch.ExchangeDeclare(exchange, c.cfg.ExchangeType, true, false, false, false, nil); err != nil {
			return fmt.Errorf("exchange declare: %w", err)
		}
	}

	args := amqp.Table{}
	if c.cfg.DeadLetterEx != "" {
		args["x-dead-letter-exchange"] = c.cfg.DeadLetterEx
	}
	if _, err := ch.QueueDeclare(
		c.cfg.QueueName(exchange), // name
		true,                      // durable
		false,                     // autoDelete
		false,                     // exclusive
		false,                     // noWait
		args,
	); err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	if err := ch.QueueBind(
		c.cfg.QueueName(exchange), // queue
		c.cfg.BindKey,             // routing key ("" for fanout/direct, "#" for topic)
		exchange,                  // exchange
		false,                     // noWait
		nil,
	); err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}
	return nil
}

func (c *Consumer) forward(ctx context.Context, exchange string, in <-chan amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-in:
			if !ok {
				return
			}
			c.metrics.MessagesConsumed.WithLabelValues(exchange).Inc()
			select {
			case c.out <- Delivery{Exchange: exchange, Body: d.Body, raw: d}:
			case <-ctx.Done():
				return
			}
		}
	}
}
