// Package config loads the ingester configuration from environment variables
// (12-factor). Every value has a documented default; see README "Ingester
// configuration" for the full table.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "INGESTER_"

// Config is the fully-resolved ingester configuration.
type Config struct {
	// --- AMQP / RabbitMQ ---
	AMQPURL        string   // amqp[s]://[user[:pass]@]host:port/vhost
	AMQPTokenFile  string   // path to a JWT/token file; when set and URL has no userinfo, auth uses user "shoveler" + token
	Exchanges      []string // non-WLCG exchanges to consume
	ExchangeType   string   // fanout|topic|direct|headers — used only if PassiveDeclare is false
	PassiveDeclare bool     // if true, declare exchanges passively (never redeclare with a conflicting type)
	BindKey        string   // routing/binding key; "" for fanout/direct, "#" for topic
	QueuePrefix    string   // durable queue name prefix; final queue is "<prefix>.<exchange>"
	Prefetch       int      // QoS prefetch count (per consumer)
	DeadLetterEx   string   // optional dead-letter exchange; empty disables DLX (uses nack-requeue instead)

	// --- ClickHouse ---
	CHAddrs        []string // host:port list (native protocol, default port 9000)
	CHDatabase     string
	CHTable        string // target table for inserts (Distributed or local)
	CHUsername     string
	CHPassword     string
	CHPasswordFile string
	CHSecure       bool // enable TLS to ClickHouse
	CHSkipVerify   bool // skip TLS verification (dev only)

	// --- Batching ---
	BatchSize     int
	FlushInterval time.Duration

	// --- Resilience ---
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// --- Observability ---
	MetricsAddr string // host:port for /metrics, /healthz, /readyz
	LogLevel    string // debug|info|warn|error
	LogFormat   string // json|text
}

// The four non-WLCG exchanges confirmed from upstream config.go defaults
// (amqp.exchange, amqp.exchange_cache, amqp.exchange_tcp, amqp.exchange_tpc).
// The WLCG exchanges (xrd-wlcg-events, xrd-wlcg-cache-events, xrd-wlcg-tpc-events)
// are deliberately NOT included.
var defaultExchanges = []string{
	"shoveled-xrd",
	"xrd-cache-events",
	"xrd-tcp-events",
	"xrd-tpc-events",
}

// MainExchange is the exchange that carries the fully-structured CollectorRecord.
// Records from any other configured exchange are treated as gstream events for
// dedup-id purposes (hashed on body identity rather than natural key).
const MainExchange = "shoveled-xrd"

// Load reads configuration from the environment and applies defaults.
func Load() (*Config, error) {
	c := &Config{
		AMQPURL:        env("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		AMQPTokenFile:  env("AMQP_TOKEN_FILE", ""),
		Exchanges:      splitCSV(env("EXCHANGES", strings.Join(defaultExchanges, ","))),
		ExchangeType:   env("EXCHANGE_TYPE", "fanout"),
		PassiveDeclare: envBool("EXCHANGE_PASSIVE", true),
		BindKey:        env("BIND_KEY", ""),
		QueuePrefix:    env("QUEUE_PREFIX", "osdf-clickhouse-ingester"),
		Prefetch:       envInt("PREFETCH", 2000),
		DeadLetterEx:   env("DEAD_LETTER_EXCHANGE", ""),

		CHAddrs:        splitCSV(env("CH_ADDRS", "localhost:9000")),
		CHDatabase:     env("CH_DATABASE", "xrootd"),
		CHTable:        env("CH_TABLE", "raw_records_dist"),
		CHUsername:     env("CH_USERNAME", "default"),
		CHPassword:     env("CH_PASSWORD", ""),
		CHPasswordFile: env("CH_PASSWORD_FILE", ""),
		CHSecure:       envBool("CH_SECURE", false),
		CHSkipVerify:   envBool("CH_SKIP_VERIFY", false),

		BatchSize:     envInt("BATCH_SIZE", 10000),
		FlushInterval: envDuration("FLUSH_INTERVAL", time.Second),

		ReconnectMin: envDuration("RECONNECT_MIN", 1*time.Second),
		ReconnectMax: envDuration("RECONNECT_MAX", 30*time.Second),

		MetricsAddr: env("METRICS_ADDR", ":9100"),
		LogLevel:    env("LOG_LEVEL", "info"),
		LogFormat:   env("LOG_FORMAT", "json"),
	}

	if len(c.Exchanges) == 0 {
		return nil, fmt.Errorf("no exchanges configured (%sEXCHANGES is empty)", envPrefix)
	}
	if len(c.CHAddrs) == 0 {
		return nil, fmt.Errorf("no ClickHouse addresses configured (%sCH_ADDRS is empty)", envPrefix)
	}
	if c.BatchSize <= 0 {
		return nil, fmt.Errorf("%sBATCH_SIZE must be > 0", envPrefix)
	}
	if c.FlushInterval <= 0 {
		return nil, fmt.Errorf("%sFLUSH_INTERVAL must be > 0", envPrefix)
	}

	// Resolve the ClickHouse password from a file if provided (Secret mount).
	if c.CHPasswordFile != "" {
		b, err := os.ReadFile(c.CHPasswordFile)
		if err != nil {
			return nil, fmt.Errorf("reading CH_PASSWORD_FILE: %w", err)
		}
		c.CHPassword = strings.TrimSpace(string(b))
	}

	return c, nil
}

// QueueName returns the durable queue name for a given exchange. All pods use
// the same name so they act as competing consumers sharing one queue.
func (c *Config) QueueName(exchange string) string {
	return c.QueuePrefix + "." + exchange
}

// UsesTokenAuth reports whether token-file auth should be used. It mirrors the
// upstream shoveler rule: token auth applies when the AMQP URL carries no
// userinfo and a token file is configured.
func (c *Config) UsesTokenAuth() bool {
	if c.AMQPTokenFile == "" {
		return false
	}
	return !urlHasUserInfo(c.AMQPURL)
}

func urlHasUserInfo(rawURL string) bool {
	// crude but dependency-free: look for "@" after the scheme separator.
	i := strings.Index(rawURL, "://")
	if i < 0 {
		return false
	}
	rest := rawURL[i+3:]
	at := strings.Index(rest, "@")
	slash := strings.Index(rest, "/")
	if at < 0 {
		return false
	}
	if slash >= 0 && at > slash {
		return false // '@' is in the path, not the authority
	}
	return true
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(envPrefix + key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(envPrefix + key); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(envPrefix + key); ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(envPrefix + key); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
