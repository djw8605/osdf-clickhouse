// Package chwriter batches CollectorRecords and commits them to ClickHouse over
// the native protocol using the clickhouse-go/v2 batch API.
//
// The column list below is the single source of truth for the INSERT and MUST
// stay in lockstep with clickhouse/schema/01_raw.sql. If you add a column to
// the schema, add it here (and to buildRow) in the same order.
package chwriter

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/djw8605/osdf-clickhouse/ingester/internal/config"
	"github.com/djw8605/osdf-clickhouse/ingester/internal/model"
)

// columns is the ordered column list provided on every INSERT. Columns that are
// filled by ClickHouse DEFAULT expressions (e.g. ingest_time) are intentionally
// omitted so the server populates them.
var columns = []string{
	"event_time",
	"event_id",
	"source_exchange",
	"start_time",
	"end_time",
	"operation_time",
	"server_id",
	"server_hostname",
	"server",
	"server_ip",
	"site",
	"user",
	"user_dn",
	"user_domain",
	"vo",
	"host",
	"token_subject",
	"token_username",
	"token_org",
	"token_role",
	"token_groups",
	"experiment",
	"activity",
	"filename",
	"dirname1",
	"dirname2",
	"logical_dirname",
	"protocol",
	"appinfo",
	"ipv6",
	"filesize",
	"read_operations",
	"read_single_operations",
	"read_vector_operations",
	"write_operations",
	"read",
	"read_single_bytes",
	"readv",
	"write",
	"read_min",
	"read_max",
	"read_average",
	"read_single_min",
	"read_single_max",
	"read_single_average",
	"read_vector_min",
	"read_vector_max",
	"read_vector_average",
	"write_min",
	"write_max",
	"write_average",
	"read_vector_count_min",
	"read_vector_count_max",
	"read_vector_count_average",
	"read_bytes_at_close",
	"write_bytes_at_close",
	"has_file_close_msg",
	"raw_json",
}

// Row is a single record ready for insertion, carrying the parsed struct, the
// source exchange, the computed event id and the original body bytes.
type Row struct {
	Record         *model.CollectorRecord
	SourceExchange string
	EventID        string
	EventTime      time.Time
	RawJSON        []byte
}

// Writer owns a ClickHouse connection and performs batch inserts.
type Writer struct {
	conn      driver.Conn
	fullTable string // database.table
	insertSQL string
}

// New opens a ClickHouse connection using the native protocol.
func New(cfg *config.Config) (*Writer, error) {
	opts := &clickhouse.Options{
		Addr: cfg.CHAddrs,
		Auth: clickhouse.Auth{
			Database: cfg.CHDatabase,
			Username: cfg.CHUsername,
			Password: cfg.CHPassword,
		},
		// Round-robin across the configured hosts and fail over on error; this
		// is what lets the ingester insert through any healthy cluster node.
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		Compression:      &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout:      10 * time.Second,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		Settings: clickhouse.Settings{
			// Large inserts: allow generous async parsing on the server side.
			"max_execution_time": 120,
		},
	}
	if cfg.CHSecure {
		opts.TLS = &tls.Config{InsecureSkipVerify: cfg.CHSkipVerify} //nolint:gosec // skip-verify is opt-in for dev
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse: %w", err)
	}

	full := fmt.Sprintf("%s.%s", cfg.CHDatabase, cfg.CHTable)
	w := &Writer{
		conn:      conn,
		fullTable: full,
		insertSQL: buildInsertSQL(full),
	}
	return w, nil
}

// Ping verifies connectivity (used by readiness checks and startup).
func (w *Writer) Ping(ctx context.Context) error {
	return w.conn.Ping(ctx)
}

// Close releases the connection.
func (w *Writer) Close() error {
	if w.conn == nil {
		return nil
	}
	return w.conn.Close()
}

// Insert commits the given rows as a single batch. On any error the batch is
// aborted (nothing partially committed) so the caller can nack-and-retry.
func (w *Writer) Insert(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, w.insertSQL)
	if err != nil {
		return fmt.Errorf("preparing batch: %w", err)
	}
	for i := range rows {
		if err := batch.Append(buildRow(&rows[i])...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("appending row %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("sending batch: %w", err)
	}
	return nil
}

func buildInsertSQL(fullTable string) string {
	sql := "INSERT INTO " + fullTable + " ("
	for i, c := range columns {
		if i > 0 {
			sql += ", "
		}
		sql += c
	}
	sql += ")"
	return sql
}

// buildRow flattens a Row into the ordered []any that batch.Append expects.
// The order MUST match `columns` exactly.
func buildRow(row *Row) []any {
	r := row.Record
	return []any{
		row.EventTime,                             // event_time DateTime64(3)
		row.EventID,                               // event_id String
		row.SourceExchange,                        // source_exchange LowCardinality(String)
		unixToTime(r.StartTime),                   // start_time DateTime64(3)
		unixToTime(r.EndTime),                     // end_time DateTime64(3)
		clampU32(r.OperationTime),                 // operation_time UInt32
		r.ServerID,                                // server_id
		r.ServerHostname,                          // server_hostname
		r.Server,                                  // server
		parseIP(r.ServerIP),                       // server_ip IPv6
		r.Site,                                    // site
		r.User,                                    // user
		r.UserDN,                                  // user_dn
		r.UserDomain,                              // user_domain
		r.VO,                                      // vo
		r.Host,                                    // host
		r.TokenSubject,                            // token_subject
		r.TokenUsername,                           // token_username
		r.TokenOrg,                                // token_org
		r.TokenRole,                               // token_role
		r.TokenGroups,                             // token_groups
		r.Experiment,                              // experiment
		r.Activity,                                // activity
		r.Filename,                                // filename
		r.Dirname1,                                // dirname1
		r.Dirname2,                                // dirname2
		r.LogicalDirname,                          // logical_dirname
		r.Protocol,                                // protocol
		r.AppInfo,                                 // appinfo
		boolU8(r.IPv6),                            // ipv6 UInt8
		clampU64(r.Filesize),                      // filesize UInt64
		clampU32(int64(r.ReadOperations)),         // read_operations UInt32
		clampU32(int64(r.ReadSingleOperations)),   // read_single_operations
		clampU32(int64(r.ReadVectorOperations)),   // read_vector_operations
		clampU32(int64(r.WriteOperations)),        // write_operations
		clampU64(r.Read),                          // read UInt64
		clampU64(r.ReadSingleBytes),               // read_single_bytes
		clampU64(r.Readv),                         // readv
		clampU64(r.Write),                         // write
		clampU32(int64(r.ReadMin)),                // read_min
		clampU32(int64(r.ReadMax)),                // read_max
		clampU64(r.ReadAverage),                   // read_average
		clampU32(int64(r.ReadSingleMin)),          // read_single_min
		clampU32(int64(r.ReadSingleMax)),          // read_single_max
		clampU64(r.ReadSingleAverage),             // read_single_average
		clampU32(int64(r.ReadVectorMin)),          // read_vector_min
		clampU32(int64(r.ReadVectorMax)),          // read_vector_max
		clampU64(r.ReadVectorAverage),             // read_vector_average
		clampU32(int64(r.WriteMin)),               // write_min
		clampU32(int64(r.WriteMax)),               // write_max
		clampU64(r.WriteAverage),                  // write_average
		clampU16(int64(r.ReadVectorCountMin)),     // read_vector_count_min UInt16
		clampU16(int64(r.ReadVectorCountMax)),     // read_vector_count_max UInt16
		r.ReadVectorCountAverage,                  // read_vector_count_average Float64
		clampU64(r.ReadBytesAtClose),              // read_bytes_at_close
		clampU64(r.WriteBytesAtClose),             // write_bytes_at_close
		uint8(clampU32(int64(r.HasFileCloseMsg))), // has_file_close_msg UInt8
		string(row.RawJSON),                       // raw_json String
	}
}

func unixToTime(sec int64) time.Time {
	if sec <= 0 {
		// A zero DateTime64 (epoch) is a valid, queryable sentinel for
		// "not present" (common on gstream events).
		return time.Unix(0, 0).UTC()
	}
	return time.Unix(sec, 0).UTC()
}

func parseIP(s string) net.IP {
	if s == "" {
		return net.IPv6zero
	}
	ip := net.ParseIP(s)
	if ip == nil {
		return net.IPv6zero
	}
	return ip
}

func boolU8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// clampU64/32/16 convert signed upstream values to the unsigned ClickHouse
// column types, mapping negatives (which indicate corrupt input) to 0 rather
// than wrapping to a huge value. This is documented in the README.
func clampU64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func clampU32(v int64) uint32 {
	if v < 0 {
		return 0
	}
	if v > (1<<32 - 1) {
		return 1<<32 - 1
	}
	return uint32(v)
}

func clampU16(v int64) uint16 {
	if v < 0 {
		return 0
	}
	if v > (1<<16 - 1) {
		return 1<<16 - 1
	}
	return uint16(v)
}
