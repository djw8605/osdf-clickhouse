//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	amqpc "github.com/djw8605/osdf-clickhouse/ingester/internal/amqp"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/chwriter"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/config"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/metrics"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func newTestMetrics() *metrics.Metrics {
	return metrics.New(prometheus.NewRegistry())
}

func rawConn(t *testing.T, cfg *config.Config) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.CHAddrs,
		Auth: clickhouse.Auth{Database: "default", Username: cfg.CHUsername, Password: cfg.CHPassword},
	})
	require.NoError(t, err)
	return conn
}

func setupSchema(t *testing.T, ctx context.Context, cfg *config.Config) {
	t.Helper()
	conn := rawConn(t, cfg)
	defer conn.Close()
	require.NoError(t, conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS xrootd"))
	require.NoError(t, conn.Exec(ctx, singleNodeSchema))
}

func optimize(t *testing.T, ctx context.Context, cfg *config.Config) {
	t.Helper()
	conn := rawConn(t, cfg)
	defer conn.Close()
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE xrootd.raw_records FINAL"))
}

func queryCount(t *testing.T, ctx context.Context, cfg *config.Config, query string) uint64 {
	t.Helper()
	conn := rawConn(t, cfg)
	defer conn.Close()
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, query).Scan(&n))
	return n
}

// collectAndInsert reads up to `want` deliveries, batches them, inserts once,
// and acks. Returns the number of rows inserted.
func collectAndInsert(t *testing.T, ctx context.Context, cfg *config.Config, w *chwriter.Writer, in <-chan amqpc.Delivery, want int) int {
	t.Helper()
	var rows []chwriter.Row
	var dels []amqpc.Delivery
	deadline := time.After(30 * time.Second)
	for len(rows) < want {
		select {
		case d := <-in:
			rec := &model.CollectorRecord{}
			_ = json.Unmarshal(d.Body, rec)
			isMain := d.Exchange == config.MainExchange
			rows = append(rows, chwriter.Row{
				Record:         rec,
				SourceExchange: d.Exchange,
				EventID:        model.ComputeEventID(rec, isMain, d.Body),
				EventTime:      rec.EventTime(time.Now().UTC()),
				RawJSON:        d.Body,
			})
			dels = append(dels, d)
		case <-deadline:
			t.Fatalf("timed out waiting for deliveries: got %d/%d", len(rows), want)
		}
	}
	require.NoError(t, w.Insert(ctx, rows))
	for _, d := range dels {
		require.NoError(t, d.Ack())
	}
	return len(rows)
}
