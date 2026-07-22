// Command ingester consumes the non-WLCG xrootd-monitoring collector stream
// from RabbitMQ and batch-inserts it into ClickHouse.
//
// Pipeline:
//
//	RabbitMQ (N durable queues, competing consumers)
//	  -> parse JSON -> CollectorRecord + raw body
//	  -> batch (flush on size OR time window, whichever first)
//	  -> ClickHouse native batch insert
//	  -> ack the batch's deliveries only after a successful commit
//
// Delivery is at-least-once; the ReplacingMergeTree dedup key (event_id)
// absorbs redeliveries.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	amqpc "github.com/djw8605/osdf-clickhouse/ingester/internal/amqp"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/chwriter"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/config"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/health"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/metrics"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/model"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := setupLogger(cfg)
	logger.Info("starting osdf-clickhouse ingester",
		"exchanges", cfg.Exchanges,
		"ch_addrs", cfg.CHAddrs,
		"ch_table", cfg.CHDatabase+"."+cfg.CHTable,
		"batch_size", cfg.BatchSize,
		"flush_interval", cfg.FlushInterval.String(),
	)

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	hs := health.New(cfg.MetricsAddr, reg)
	hsErr := hs.Start()
	logger.Info("metrics/health server listening", "addr", cfg.MetricsAddr)

	writer, err := chwriter.New(cfg)
	if err != nil {
		logger.Error("failed to init clickhouse writer", "error", err)
		os.Exit(1)
	}
	defer writer.Close()

	// Root context cancelled on SIGTERM/SIGINT.
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Wait for ClickHouse to be reachable before consuming (readiness gate).
	if err := waitForClickHouse(rootCtx, writer, m, logger); err != nil {
		logger.Error("clickhouse never became reachable", "error", err)
		os.Exit(1)
	}
	hs.SetReady(true)
	m.Ready.Set(1)

	// Consumer runs under its own context so we can keep the AMQP connection
	// alive during the final flush (acks must succeed) and only tear it down
	// after the batcher has drained.
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	consumer := amqpc.New(cfg, logger, m)
	go consumer.Run(consumerCtx)

	b := &batcher{cfg: cfg, log: logger, m: m, writer: writer, health: hs}

	// Run the batcher until SIGTERM (rootCtx) or a fatal metrics-server error.
	go func() {
		if err := <-hsErr; err != nil {
			logger.Error("metrics/health server failed", "error", err)
			stop() // trigger graceful shutdown
		}
	}()

	b.run(rootCtx, consumer.Deliveries())

	logger.Info("shutting down: tearing down consumer")
	consumerCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := hs.Shutdown(shutdownCtx); err != nil {
		logger.Warn("metrics server shutdown error", "error", err)
	}
	logger.Info("ingester stopped cleanly")
}

// batcher accumulates deliveries and flushes them to ClickHouse.
type batcher struct {
	cfg    *config.Config
	log    *slog.Logger
	m      *metrics.Metrics
	writer *chwriter.Writer
	health *health.Server

	rows       []chwriter.Row
	deliveries []amqpc.Delivery
}

// run consumes until ctx is cancelled, then performs a final flush of the
// in-flight batch (the AMQP connection is still alive at this point so acks
// succeed) and returns. Buffered-but-unread messages are left unacked and are
// safely redelivered on the next run (absorbed by the dedup key).
func (b *batcher) run(ctx context.Context, in <-chan amqpc.Delivery) {
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.log.Info("stop signal received, flushing final batch", "rows", len(b.rows))
			b.finalFlush()
			return

		case d, ok := <-in:
			if !ok {
				// Consumer closed the channel unexpectedly; flush and return.
				b.finalFlush()
				return
			}
			b.add(d)
			if len(b.rows) >= b.cfg.BatchSize {
				b.flush(ctx)
			}

		case <-ticker.C:
			if len(b.rows) > 0 {
				b.flush(ctx)
			}
		}
	}
}

// finalFlush flushes the in-flight batch during shutdown with a bounded
// deadline, so a shutdown never hangs if ClickHouse is unreachable. It runs
// while the AMQP connection is still alive, so acks succeed; if the flush
// deadline is hit the batch is nack-requeued and redelivered next run.
func (b *batcher) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b.flush(ctx)
}

// add parses a delivery and appends it to the current batch. Parse failures are
// counted but NOT dropped: the raw body is still stored so nothing is lost.
func (b *batcher) add(d amqpc.Delivery) {
	recv := time.Now().UTC()
	rec := &model.CollectorRecord{}
	if err := json.Unmarshal(d.Body, rec); err != nil {
		b.m.ParseErrors.Inc()
		b.log.Debug("json parse error; storing raw body", "exchange", d.Exchange, "error", err)
		// rec stays zero-valued; raw_json + event_time(receive) preserve the message.
	}
	isMain := d.Exchange == config.MainExchange
	row := chwriter.Row{
		Record:         rec,
		SourceExchange: d.Exchange,
		EventID:        model.ComputeEventID(rec, isMain, d.Body),
		EventTime:      rec.EventTime(recv),
		RawJSON:        d.Body,
	}
	b.rows = append(b.rows, row)
	b.deliveries = append(b.deliveries, d)
	b.m.CurrentBatchSize.Set(float64(len(b.rows)))
}

// flush commits the current batch, retrying with backoff until it succeeds or
// the context is cancelled. On success it acks every delivery. On unrecoverable
// failure during shutdown it nacks-with-requeue so the broker redelivers.
func (b *batcher) flush(ctx context.Context) {
	if len(b.rows) == 0 {
		return
	}
	n := len(b.rows)
	backoff := b.cfg.ReconnectMin
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := b.writer.Insert(ctx, b.rows)
		if err == nil {
			b.m.InsertLatency.Observe(time.Since(start).Seconds())
			b.m.RowsInserted.Add(float64(n))
			b.m.BatchesCommitted.Inc()
			b.ackAll()
			b.health.SetReady(true)
			b.m.Ready.Set(1)
			b.reset()
			b.log.Debug("batch committed", "rows", n, "attempt", attempt)
			return
		}

		b.m.InsertFailures.Inc()
		b.health.SetReady(false)
		b.m.Ready.Set(0)
		b.log.Warn("batch insert failed", "rows", n, "attempt", attempt, "error", err)

		// If the parent context is cancelled (shutdown) do not loop forever:
		// requeue the batch and let the next run pick it up.
		if ctx.Err() != nil {
			b.log.Warn("context cancelled during flush; requeuing batch", "rows", n)
			b.nackAll()
			b.reset()
			return
		}

		select {
		case <-ctx.Done():
			b.nackAll()
			b.reset()
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > b.cfg.ReconnectMax {
			backoff = b.cfg.ReconnectMax
		}
	}
}

func (b *batcher) ackAll() {
	for _, d := range b.deliveries {
		if err := d.Ack(); err != nil {
			// Ack failed (e.g. channel died between insert and ack). The row is
			// committed; the broker will redeliver and the dedup key absorbs it.
			b.log.Debug("ack failed; relying on dedup", "error", err)
		}
	}
}

func (b *batcher) nackAll() {
	requeue := b.cfg.DeadLetterEx == "" // if a DLX is set, nack without requeue routes to it
	for _, d := range b.deliveries {
		if err := d.Nack(requeue); err != nil {
			b.log.Debug("nack failed", "error", err)
		} else if !requeue {
			b.m.DeadLetters.Inc()
		}
	}
}

func (b *batcher) reset() {
	b.rows = b.rows[:0]
	b.deliveries = b.deliveries[:0]
	b.m.CurrentBatchSize.Set(0)
}

// waitForClickHouse pings ClickHouse until reachable or ctx is cancelled.
func waitForClickHouse(ctx context.Context, w *chwriter.Writer, m *metrics.Metrics, log *slog.Logger) error {
	backoff := time.Second
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := w.Ping(pingCtx)
		cancel()
		if err == nil {
			log.Info("clickhouse reachable")
			return nil
		}
		m.CHReconnects.Inc()
		log.Warn("waiting for clickhouse", "error", err, "backoff", backoff.String())
		select {
		case <-ctx.Done():
			return errors.New("context cancelled while waiting for clickhouse")
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func setupLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h)
}
