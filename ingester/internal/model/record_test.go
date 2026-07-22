package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A representative main-stream CollectorRecord body (subset of fields present;
// the rest default to zero). Mirrors the upstream JSON tags.
const sampleMain = `{
  "@timestamp": "2026-07-22T12:34:56.789Z",
  "start_time": 1753183000,
  "end_time": 1753183100,
  "operation_time": 100,
  "serverID": "abc123",
  "server_hostname": "xrootd-1.example.org",
  "server": "xrootd-1.example.org:1094",
  "server_ip": "2001:db8::1",
  "site": "MY_SITE",
  "user": "alice",
  "user_dn": "/DC=org/CN=alice",
  "vo": "osg",
  "host": "client.example.org",
  "filename": "/data/file.root",
  "logical_dirname": "/data",
  "protocol": "https",
  "ipv6": true,
  "filesize": 1048576,
  "read_operations": 42,
  "read": 999999,
  "write": 0,
  "read_vector_count_average": 3.5,
  "HasFileCloseMsg": 1
}`

func TestParseMainRecord(t *testing.T) {
	var rec CollectorRecord
	require.NoError(t, json.Unmarshal([]byte(sampleMain), &rec))

	assert.Equal(t, "abc123", rec.ServerID)
	assert.Equal(t, "xrootd-1.example.org", rec.ServerHostname)
	assert.Equal(t, "2001:db8::1", rec.ServerIP)
	assert.Equal(t, "osg", rec.VO)
	assert.Equal(t, "/data/file.root", rec.Filename)
	assert.Equal(t, int64(1048576), rec.Filesize)
	assert.Equal(t, int32(42), rec.ReadOperations)
	assert.Equal(t, int64(999999), rec.Read)
	assert.Equal(t, true, rec.IPv6)
	assert.Equal(t, 3.5, rec.ReadVectorCountAverage)
	assert.Equal(t, 1, rec.HasFileCloseMsg)

	want := time.Date(2026, 7, 22, 12, 34, 56, 789_000_000, time.UTC)
	assert.True(t, rec.Timestamp.Equal(want), "timestamp mismatch: got %s", rec.Timestamp)
}

func TestEventTimeFallbacks(t *testing.T) {
	fallback := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Prefers @timestamp.
	r1 := CollectorRecord{Timestamp: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), EndTime: 111}
	assert.True(t, r1.EventTime(fallback).Equal(r1.Timestamp))

	// Falls back to end_time.
	r2 := CollectorRecord{EndTime: 1753183100}
	assert.True(t, r2.EventTime(fallback).Equal(time.Unix(1753183100, 0).UTC()))

	// Falls back to start_time.
	r3 := CollectorRecord{StartTime: 1753183000}
	assert.True(t, r3.EventTime(fallback).Equal(time.Unix(1753183000, 0).UTC()))

	// Falls back to the supplied receive time.
	r4 := CollectorRecord{}
	assert.True(t, r4.EventTime(fallback).Equal(fallback))
}

func TestComputeEventIDDeterministic(t *testing.T) {
	var rec CollectorRecord
	require.NoError(t, json.Unmarshal([]byte(sampleMain), &rec))

	body := []byte(sampleMain)
	id1 := ComputeEventID(&rec, true, body)
	id2 := ComputeEventID(&rec, true, body)
	assert.Equal(t, id1, id2, "same record must yield same id (redelivery dedup)")
	assert.Len(t, id1, 32, "id is 128-bit hex (32 chars)")
}

func TestComputeEventIDDistinctForDifferentRecords(t *testing.T) {
	base := CollectorRecord{ServerID: "s1", Filename: "/a", StartTime: 1, EndTime: 2, Read: 10}
	other := base
	other.Read = 11 // different byte counter -> different natural key

	idBase := ComputeEventID(&base, true, nil)
	idOther := ComputeEventID(&other, true, nil)
	assert.NotEqual(t, idBase, idOther)
}

func TestComputeEventIDMainIgnoresBody(t *testing.T) {
	// For main-stream records the id is derived from the struct fields, not the
	// raw bytes, so two byte-different encodings of the same record dedup.
	rec := CollectorRecord{ServerID: "s1", Filename: "/a", StartTime: 1, EndTime: 2}
	id1 := ComputeEventID(&rec, true, []byte(`{"a":1}`))
	id2 := ComputeEventID(&rec, true, []byte(`{"b":2}`))
	assert.Equal(t, id1, id2)
}

func TestComputeEventIDGstreamUsesBody(t *testing.T) {
	// gstream events have empty natural-key fields, so the id must come from the
	// body. Identical bodies dedup; different bodies do not collide.
	empty := CollectorRecord{}
	bodyA := []byte(`{"file_path":"/cache/x","access_count":3}`)
	bodyB := []byte(`{"file_path":"/cache/y","access_count":9}`)

	idA1 := ComputeEventID(&empty, false, bodyA)
	idA2 := ComputeEventID(&empty, false, bodyA)
	idB := ComputeEventID(&empty, false, bodyB)

	assert.Equal(t, idA1, idA2, "identical gstream bodies must dedup")
	assert.NotEqual(t, idA1, idB, "distinct gstream bodies must not collide")
}

// A gstream cache event carries a different shape; parsing into CollectorRecord
// must not error and must leave the struct mostly zero-valued while raw body is
// preserved by the caller.
func TestParseGstreamEventDoesNotError(t *testing.T) {
	body := `{"file_path":"/cache/data.root","block_size":131072,"access_count":7,"serverID":"srv"}`
	var rec CollectorRecord
	require.NoError(t, json.Unmarshal([]byte(body), &rec))
	// Overlapping field is captured; non-overlapping cache fields are ignored.
	assert.Equal(t, "srv", rec.ServerID)
	assert.Equal(t, "", rec.Filename)
	assert.Equal(t, int64(0), rec.Read)
}
