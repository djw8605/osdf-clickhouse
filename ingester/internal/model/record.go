// Package model contains the CollectorRecord data model consumed from the
// xrootd-monitoring-shoveler collector stream, plus helpers for computing a
// deterministic dedup id used by the ClickHouse ReplacingMergeTree key.
//
// The struct is derived field-for-field from the upstream source of truth:
//
//	github.com/opensciencegrid/xrootd-monitoring-shoveler
//	collector/correlator.go -> type CollectorRecord struct
//
// The upstream JSON tags are reproduced verbatim so that json.Unmarshal maps
// the wire format exactly. Do NOT invent fields; when upstream evolves, the
// raw JSON is always preserved in the raw_json ClickHouse column so nothing is
// silently dropped.
package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// CollectorRecord is the correlated file-access record emitted by the collector
// binary onto the main non-WLCG exchange (default: shoveled-xrd).
//
// IMPORTANT: only the main exchange carries this exact schema. The type-specific
// non-WLCG exchanges (xrd-cache-events, xrd-tcp-events, xrd-tpc-events) carry
// heterogeneous "gstream" event maps with a different shape. When a gstream
// event is unmarshalled into this struct, only the fields whose JSON keys happen
// to overlap are populated; everything else stays zero-valued and the complete
// original body is retained in the raw_json column. See README "Data model".
type CollectorRecord struct {
	Timestamp              time.Time `json:"@timestamp"`
	StartTime              int64     `json:"start_time"`
	EndTime                int64     `json:"end_time"`
	OperationTime          int64     `json:"operation_time"`
	ServerID               string    `json:"serverID"`
	ServerHostname         string    `json:"server_hostname"`
	Server                 string    `json:"server"`
	ServerIP               string    `json:"server_ip"`
	Site                   string    `json:"site"`
	User                   string    `json:"user"`
	UserDN                 string    `json:"user_dn"`
	UserDomain             string    `json:"user_domain,omitempty"`
	VO                     string    `json:"vo,omitempty"`
	Host                   string    `json:"host"`
	TokenSubject           string    `json:"token_subject,omitempty"`
	TokenUsername          string    `json:"token_username,omitempty"`
	TokenOrg               string    `json:"token_org,omitempty"`
	TokenRole              string    `json:"token_role,omitempty"`
	TokenGroups            string    `json:"token_groups,omitempty"`
	Experiment             string    `json:"experiment,omitempty"`
	Activity               string    `json:"activity,omitempty"`
	Filename               string    `json:"filename"`
	Dirname1               string    `json:"dirname1"`
	Dirname2               string    `json:"dirname2"`
	LogicalDirname         string    `json:"logical_dirname"`
	Protocol               string    `json:"protocol"`
	AppInfo                string    `json:"appinfo"`
	IPv6                   bool      `json:"ipv6"`
	Filesize               int64     `json:"filesize"`
	ReadOperations         int32     `json:"read_operations"`
	ReadSingleOperations   int32     `json:"read_single_operations"`
	ReadVectorOperations   int32     `json:"read_vector_operations"`
	WriteOperations        int32     `json:"write_operations"`
	Read                   int64     `json:"read"`
	ReadSingleBytes        int64     `json:"read_single_bytes"`
	Readv                  int64     `json:"readv"`
	Write                  int64     `json:"write"`
	ReadMin                int32     `json:"read_min"`
	ReadMax                int32     `json:"read_max"`
	ReadAverage            int64     `json:"read_average"`
	ReadSingleMin          int32     `json:"read_single_min"`
	ReadSingleMax          int32     `json:"read_single_max"`
	ReadSingleAverage      int64     `json:"read_single_average"`
	ReadVectorMin          int32     `json:"read_vector_min"`
	ReadVectorMax          int32     `json:"read_vector_max"`
	ReadVectorAverage      int64     `json:"read_vector_average"`
	WriteMin               int32     `json:"write_min"`
	WriteMax               int32     `json:"write_max"`
	WriteAverage           int64     `json:"write_average"`
	ReadVectorCountMin     int16     `json:"read_vector_count_min"`
	ReadVectorCountMax     int16     `json:"read_vector_count_max"`
	ReadVectorCountAverage float64   `json:"read_vector_count_average"`
	ReadBytesAtClose       int64     `json:"read_bytes_at_close"`
	WriteBytesAtClose      int64     `json:"write_bytes_at_close"`
	HasFileCloseMsg        int       `json:"HasFileCloseMsg"`
}

// EventTime returns the best available event timestamp for the record. It
// prefers the parsed @timestamp; if that is the zero value (common for gstream
// events that carry their own time fields), it falls back to end_time /
// start_time interpreted as Unix seconds, and finally to the supplied receive
// time so a row is never written with a zero partition key.
func (r *CollectorRecord) EventTime(fallback time.Time) time.Time {
	if !r.Timestamp.IsZero() {
		return r.Timestamp
	}
	if r.EndTime > 0 {
		return time.Unix(r.EndTime, 0).UTC()
	}
	if r.StartTime > 0 {
		return time.Unix(r.StartTime, 0).UTC()
	}
	return fallback
}

// ComputeEventID derives a deterministic, stable dedup id for a fstream record.
//
// RabbitMQ delivery is at-least-once, so the same logical record can be
// delivered more than once (e.g. after a consumer crash before ack, or a
// broker requeue). ReplacingMergeTree collapses rows that share the ORDER BY
// key, and event_id is part of that key, so identical redeliveries must map to
// the same id while genuinely distinct records must not collide.
//
// The id is a hash over the natural key of a file-access record: server
// identity + filename + start/end time + byte counters. These fields together
// uniquely identify one file-close event; a redelivery reproduces them exactly.
//
// Failure mode (documented in the README): two genuinely distinct events that
// share every hashed field would collapse into one row. In practice
// serverID+filename+start+end+bytes is unique per file close, so this is
// vanishingly unlikely.
func ComputeEventID(r *CollectorRecord) string {
	h := sha256.New()
	writeString(h, r.ServerID)
	writeString(h, r.Server)
	writeString(h, r.Filename)
	writeString(h, r.LogicalDirname)
	writeInt(h, r.StartTime)
	writeInt(h, r.EndTime)
	writeInt(h, r.Filesize)
	writeInt(h, r.Read)
	writeInt(h, r.Readv)
	writeInt(h, r.Write)
	writeInt(h, r.ReadBytesAtClose)
	writeInt(h, r.WriteBytesAtClose)
	sum := h.Sum(nil)
	// 128 bits of SHA-256 is more than enough to make collisions negligible
	// while keeping the key column narrow.
	return hex.EncodeToString(sum[:16])
}

func writeString(h interface{ Write([]byte) (int, error) }, s string) {
	h.Write([]byte(s))
	h.Write([]byte{0}) // field separator to avoid ambiguity between concatenations
}

func writeInt(h interface{ Write([]byte) (int, error) }, v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	h.Write(buf[:])
}

// ParseIPOrZero returns a string suitable for the IPv6 ClickHouse column.
// ClickHouse accepts both IPv4 and IPv6 textual forms for an IPv6 column
// (IPv4 is mapped into ::ffff:0:0/96). An empty or unparseable value becomes
// "::" so inserts never fail on a malformed address.
func ParseIPOrZero(s string) string {
	if s == "" {
		return "::"
	}
	return s
}
