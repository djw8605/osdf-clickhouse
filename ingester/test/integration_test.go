//go:build integration

// Package integration contains an end-to-end test that stands up real RabbitMQ
// and ClickHouse containers with testcontainers-go, runs the ingester pipeline,
// and asserts that published records are batched, inserted, and deduplicated.
//
// Run with:
//
//	go test -tags=integration ./test/...
//
// Requires a working Docker daemon. It is excluded from the default build so
// `go test ./...` stays fast and dependency-light for CI unit runs.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	amqpc "github.com/djw8605/osdf-clickhouse/ingester/internal/amqp"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/chwriter"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/config"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/model"
)

const testExchange = "shoveled-xrd"

// singleNodeSchema is a non-replicated, single-node version of the raw table
// used only for the integration test (the production schema is
// ReplicatedReplacingMergeTree ON CLUSTER — see clickhouse/schema/).
const singleNodeSchema = `
CREATE TABLE IF NOT EXISTS xrootd.raw_records
(
    event_time              DateTime64(3),
    event_id                String,
    source_exchange         LowCardinality(String),
    start_time              DateTime64(3),
    end_time                DateTime64(3),
    operation_time          UInt32,
    server_id               LowCardinality(String),
    server_hostname         LowCardinality(String),
    server                  String,
    server_ip               IPv6,
    site                    LowCardinality(String),
    user                    String,
    user_dn                 String,
    user_domain             LowCardinality(String),
    vo                      LowCardinality(String),
    host                    String,
    token_subject           String,
    token_username          String,
    token_org               LowCardinality(String),
    token_role              LowCardinality(String),
    token_groups            String,
    experiment              LowCardinality(String),
    activity                LowCardinality(String),
    filename                String,
    dirname1                String,
    dirname2                String,
    logical_dirname         String,
    protocol                LowCardinality(String),
    appinfo                 String,
    ipv6                    UInt8,
    filesize                UInt64,
    read_operations         UInt32,
    read_single_operations  UInt32,
    read_vector_operations  UInt32,
    write_operations        UInt32,
    read                    UInt64,
    read_single_bytes       UInt64,
    readv                   UInt64,
    write                   UInt64,
    read_min                UInt32,
    read_max                UInt32,
    read_average            UInt64,
    read_single_min         UInt32,
    read_single_max         UInt32,
    read_single_average     UInt64,
    read_vector_min         UInt32,
    read_vector_max         UInt32,
    read_vector_average     UInt64,
    write_min               UInt32,
    write_max               UInt32,
    write_average           UInt64,
    read_vector_count_min   UInt16,
    read_vector_count_max   UInt16,
    read_vector_count_average Float64,
    read_bytes_at_close     UInt64,
    write_bytes_at_close    UInt64,
    has_file_close_msg      UInt8,
    raw_json                String,
    ingest_time             DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (server_hostname, event_time, event_id)
`

func TestEndToEnd(t *testing.T) {
	ctx := context.Background()

	// --- RabbitMQ container ---
	rmq, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3.13-management",
			ExposedPorts: []string{"5672/tcp"},
			WaitingFor:   wait.ForListeningPort("5672/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	defer rmq.Terminate(ctx)

	rmqHost, err := rmq.Host(ctx)
	require.NoError(t, err)
	rmqPort, err := rmq.MappedPort(ctx, "5672/tcp")
	require.NoError(t, err)
	amqpURL := fmt.Sprintf("amqp://guest:guest@%s:%s/", rmqHost, rmqPort.Port())

	// --- ClickHouse container ---
	ch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "clickhouse/clickhouse-server:24.8",
			ExposedPorts: []string{"9000/tcp", "8123/tcp"},
			WaitingFor:   wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	defer ch.Terminate(ctx)

	chHost, err := ch.Host(ctx)
	require.NoError(t, err)
	chPort, err := ch.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)

	// --- Config pointing at the containers ---
	cfg := &config.Config{
		AMQPURL:        amqpURL,
		Exchanges:      []string{testExchange},
		ExchangeType:   "fanout",
		PassiveDeclare: false, // declare it ourselves in this test
		BindKey:        "",
		QueuePrefix:    "test-ingester",
		Prefetch:       100,
		CHAddrs:        []string{fmt.Sprintf("%s:%s", chHost, chPort.Port())},
		CHDatabase:     "xrootd",
		CHTable:        "raw_records",
		CHUsername:     "default",
		BatchSize:      100,
		FlushInterval:  200 * time.Millisecond,
		ReconnectMin:   200 * time.Millisecond,
		ReconnectMax:   2 * time.Second,
	}

	// --- Schema ---
	setupSchema(t, ctx, cfg)

	// --- Publish two identical records (tests dedup) + one distinct ---
	publish(t, amqpURL, sampleRecord("/data/a.root", 100))
	publish(t, amqpURL, sampleRecord("/data/a.root", 100)) // duplicate
	publish(t, amqpURL, sampleRecord("/data/b.root", 200)) // distinct

	// --- Run the writer + consumer directly ---
	writer, err := chwriter.New(cfg)
	require.NoError(t, err)
	defer writer.Close()
	require.NoError(t, writer.Ping(ctx))

	m := newTestMetrics()
	consumer := amqpc.New(cfg, testLogger(), m)
	consumerCtx, cancel := context.WithCancel(ctx)
	go consumer.Run(consumerCtx)

	// Collect deliveries into a batch and insert.
	got := collectAndInsert(t, ctx, cfg, writer, consumer.Deliveries(), 3)
	cancel()
	require.GreaterOrEqual(t, got, 3)

	// --- Assert dedup: OPTIMIZE FINAL then count distinct filenames ---
	require.NoError(t, writer.Ping(ctx))
	optimize(t, ctx, cfg)

	total := queryCount(t, ctx, cfg, "SELECT count() FROM xrootd.raw_records FINAL")
	require.Equal(t, uint64(2), total, "3 inserts (1 duplicate) must collapse to 2 rows via ReplacingMergeTree")
}

func sampleRecord(filename string, read int64) []byte {
	rec := model.CollectorRecord{
		Timestamp:      time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		StartTime:      1753183000,
		EndTime:        1753183100,
		ServerID:       "srv-1",
		ServerHostname: "xrootd-1.example.org",
		ServerIP:       "2001:db8::1",
		VO:             "osg",
		Filename:       filename,
		Read:           read,
		Filesize:       1 << 20,
	}
	b, _ := json.Marshal(rec)
	return b
}

func publish(t *testing.T, amqpURL string, body []byte) {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)
	defer conn.Close()
	c, err := conn.Channel()
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.ExchangeDeclare(testExchange, "fanout", true, false, false, false, nil))
	require.NoError(t, c.PublishWithContext(context.Background(), testExchange, "", false, false,
		amqp.Publishing{ContentType: "text/plain", Body: body}))
}

// Note: setupSchema, optimize, queryCount, collectAndInsert, newTestMetrics and
// testLogger are defined in helpers_test.go.
