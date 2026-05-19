package utils

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// DefaultTransportConfig holds the timeout configuration for the robust transport.
// These values are tuned to prevent TCP-level hangs that can cause goroutine leaks
// in long-running streaming scenarios (e.g. LLM SSE streams).
type DefaultTransportConfig struct {
	// DialTimeout is the maximum time to establish a TCP connection (DNS + connect).
	DialTimeout time.Duration

	// TLSHandshakeTimeout is the maximum time for the TLS handshake.
	TLSHandshakeTimeout time.Duration

	// ResponseHeaderTimeout is the maximum time to wait for the first response header
	// after the request is fully sent. For streaming, this covers the time until the
	// server starts sending data (not the total stream duration).
	ResponseHeaderTimeout time.Duration

	// IdleConnTimeout is how long idle connections stay in the pool before being closed.
	IdleConnTimeout time.Duration

	// ExpectContinueTimeout is the time to wait for a "100 Continue" response.
	ExpectContinueTimeout time.Duration

	// MaxIdleConns is the maximum number of idle connections across all hosts.
	MaxIdleConns int

	// MaxIdleConnsPerHost is the maximum number of idle connections per host.
	MaxIdleConnsPerHost int

	// DisableHTTP2 disables HTTP/2 to prevent multiplexing issues where a single
	// stuck connection can block multiple requests.
	DisableHTTP2 bool
}

// DefaultRobustConfig returns the recommended transport configuration for LLM API calls.
// These values are specifically tuned to prevent the deadlock scenarios observed in
// discuss sessions where TCP connections hang indefinitely.
func DefaultRobustConfig() DefaultTransportConfig {
	return DefaultTransportConfig{
		DialTimeout:           30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		DisableHTTP2:          true,
	}
}

// NewRobustTransport creates an *http.Transport with aggressive timeout settings
// designed to prevent goroutine leaks caused by hung TCP connections.
//
// Key design decisions:
//   - HTTP/2 is disabled by default because its multiplexing can cause a single
//     stuck connection to block multiple concurrent requests.
//   - ResponseHeaderTimeout is set to 60s to accommodate LLM providers that may
//     take time to start generating tokens, while still preventing indefinite hangs.
//   - All dial/TLS timeouts are set to prevent connection-establishment hangs.
//   - The transport respects context cancellation at every stage (dial, TLS, header wait).
func NewRobustTransport() *http.Transport {
	return NewRobustTransportWithConfig(DefaultRobustConfig())
}

// NewRobustTransportWithConfig creates an *http.Transport with the given configuration.
func NewRobustTransportWithConfig(cfg DefaultTransportConfig) *http.Transport {
	t := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ExpectContinueTimeout: cfg.ExpectContinueTimeout,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		ForceAttemptHTTP2:     !cfg.DisableHTTP2,
	}

	// When HTTP/2 is disabled, explicitly configure TLS to not negotiate h2.
	if cfg.DisableHTTP2 {
		t.TLSClientConfig = &tls.Config{
			NextProtos: []string{"http/1.1"},
		}
	}

	return t
}

// DefaultTransport is a pre-configured robust transport instance suitable for
// most LLM API call scenarios. It is safe for concurrent use.
var DefaultTransport = NewRobustTransport()

// NewRobustHTTPClient creates an *http.Client using the robust transport.
// Timeout is set to 0 (no client-level timeout) because streaming responses
// can legitimately take a long time. Timeout control should be done via
// context.WithTimeout/context.WithDeadline at the call site.
func NewRobustHTTPClient() *http.Client {
	return &http.Client{
		Transport: NewRobustTransport(),
		Timeout:   0, // Rely on context for timeout control in streaming scenarios
	}
}

// EnsureRobustTransport checks if the given http.Client has a nil Transport
// and injects the robust transport if so. This is useful for backward compatibility
// when external code passes in a custom client without configuring the transport.
func EnsureRobustTransport(client *http.Client) *http.Client {
	if client == nil {
		return NewRobustHTTPClient()
	}
	if client.Transport == nil {
		client.Transport = NewRobustTransport()
	}
	return client
}
