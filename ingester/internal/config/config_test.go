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

	assert.Equal(t, defaultExchanges, c.Exchanges)
	assert.Equal(t, "fanout", c.ExchangeType)
	assert.True(t, c.PassiveDeclare)
	assert.Equal(t, "", c.BindKey)
	assert.Equal(t, 10000, c.BatchSize)
	assert.Equal(t, time.Second, c.FlushInterval)
	assert.Equal(t, "xrootd", c.CHDatabase)
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

func TestUsesTokenAuth(t *testing.T) {
	// URL with userinfo -> plain auth, even if token file present.
	c := &Config{AMQPURL: "amqps://user:pass@broker:5671/", AMQPTokenFile: "/x/token"}
	assert.False(t, c.UsesTokenAuth())

	// URL without userinfo + token file -> token auth.
	c2 := &Config{AMQPURL: "amqps://broker.example.org:5671/vhost", AMQPTokenFile: "/x/token"}
	assert.True(t, c2.UsesTokenAuth())

	// No token file -> never token auth.
	c3 := &Config{AMQPURL: "amqps://broker:5671/"}
	assert.False(t, c3.UsesTokenAuth())
}

func TestURLHasUserInfo(t *testing.T) {
	assert.True(t, urlHasUserInfo("amqp://u:p@host:5672/"))
	assert.True(t, urlHasUserInfo("amqp://u@host:5672/"))
	assert.False(t, urlHasUserInfo("amqp://host:5672/"))
	assert.False(t, urlHasUserInfo("amqp://host:5672/vhost@name")) // '@' in path only
}
