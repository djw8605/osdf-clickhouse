package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	// No env set: expect documented defaults.
	c, err := Load()
	require.NoError(t, err)

	// fstream only: the single main exchange, no gstream/WLCG exchanges.
	assert.Equal(t, []string{"shoveled-xrd"}, c.Exchanges)
	assert.Equal(t, "fanout", c.ExchangeType)
	assert.True(t, c.PassiveDeclare)
	assert.Equal(t, "", c.BindKey)
	assert.Equal(t, 10000, c.BatchSize)
	assert.Equal(t, time.Second, c.FlushInterval)
	assert.Equal(t, "xrootd", c.CHDatabase)
	assert.NotContains(t, c.Exchanges, "xrd-cache-events", "gstream exchanges must not be consumed")
	assert.NotContains(t, c.Exchanges, "xrd-wlcg-events", "WLCG exchanges must not be consumed")
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("INGESTER_EXCHANGES", "a, b ,c")
	t.Setenv("INGESTER_BATCH_SIZE", "500")
	t.Setenv("INGESTER_FLUSH_INTERVAL", "250ms")
	t.Setenv("INGESTER_CH_ADDRS", "ch1:9000, ch2:9000")
	t.Setenv("INGESTER_EXCHANGE_PASSIVE", "false")

	c, err := Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, c.Exchanges)
	assert.Equal(t, 500, c.BatchSize)
	assert.Equal(t, 250*time.Millisecond, c.FlushInterval)
	assert.Equal(t, []string{"ch1:9000", "ch2:9000"}, c.CHAddrs)
	assert.False(t, c.PassiveDeclare)
}

func TestQueueName(t *testing.T) {
	c := &Config{QueuePrefix: "osdf-clickhouse-ingester"}
	assert.Equal(t, "osdf-clickhouse-ingester.shoveled-xrd", c.QueueName("shoveled-xrd"))
}
