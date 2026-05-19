package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a context-aware HTTP client wrapper designed for LLM API calls.
// It wraps http.Client with BaseURL support, automatic body serialization,
// debug logging, and robust transport configuration.
//
// Key design principles:
//   - All requests are bound to a context via http.NewRequestWithContext
//   - http.Client.Timeout is set to 0 (streaming relies on context deadline)
//   - Debug logs are emitted before and after each request
//   - Multiple body types are automatically serialized to JSON
type Client struct {
	http.Client
	BaseURL string
	Logger  *slog.Logger
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithBaseURL sets the base URL for all requests.
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) {
		c.BaseURL = baseURL
	}
}

// WithLogger sets the logger for debug output.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		c.Logger = logger
	}
}

// WithTransport sets a custom transport on the client.
func WithTransport(transport http.RoundTripper) ClientOption {
	return func(c *Client) {
		c.Client.Transport = transport
	}
}

// WithHTTPClient replaces the underlying http.Client entirely.
// The transport will be checked and upgraded if nil.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		if hc != nil {
			c.Client = *hc
			// Ensure robust transport if none provided
			if c.Client.Transport == nil {
				c.Client.Transport = NewRobustTransport()
			}
		}
	}
}

// NewClient creates a new Client with robust defaults.
func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		Client: http.Client{
			Transport: NewRobustTransport(),
			Timeout:   0, // Rely on context for timeout; streaming can be long-lived
		},
		Logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// resolveURL joins BaseURL and path into a full URL.
func (c *Client) resolveURL(path string) (string, error) {
	if c.BaseURL == "" {
		return path, nil
	}
	// If path is already a full URL, use it directly
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path, nil
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base URL %q: %w", c.BaseURL, err)
	}
	return u.JoinPath(path).String(), nil
}

// serializeBody converts various body types into an io.Reader and content type.
func serializeBody(body any) (io.Reader, string, error) {
	if body == nil {
		return nil, "", nil
	}
	switch v := body.(type) {
	case io.Reader:
		return v, "", nil
	case []byte:
		return bytes.NewReader(v), "", nil
	case string:
		return strings.NewReader(v), "", nil
	case map[string]any:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, "", fmt.Errorf("marshal map body: %w", err)
		}
		return bytes.NewReader(data), "application/json", nil
	case []map[string]any:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, "", fmt.Errorf("marshal slice body: %w", err)
		}
		return bytes.NewReader(data), "application/json", nil
	default:
		// Default: marshal as JSON
		data, err := json.Marshal(v)
		if err != nil {
			return nil, "", fmt.Errorf("marshal body: %w", err)
		}
		return bytes.NewReader(data), "application/json", nil
	}
}

// Request sends an HTTP request with full context awareness.
// It supports multiple body types (io.Reader, []byte, string, map, struct → JSON).
// Debug logs are emitted before and after the request.
func (c *Client) Request(ctx context.Context, method string, path string, body any, headers map[string]string) (*http.Response, error) {
	fullURL, err := c.resolveURL(path)
	if err != nil {
		return nil, err
	}

	bodyReader, contentType, err := serializeBody(body)
	if err != nil {
		return nil, err
	}

	c.Logger.Debug("[HTTP] Start Request", slog.String("method", method), slog.String("url", fullURL))
	startTime := time.Now()

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set content type if auto-detected from body serialization
	if contentType != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}

	// Apply custom headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.Client.Do(req)
	elapsed := time.Since(startTime)

	var statusCode int
	if resp != nil {
		statusCode = resp.StatusCode
	}

	c.Logger.Debug("[HTTP] END Request",
		slog.String("method", method),
		slog.String("url", fullURL),
		slog.Int("status", statusCode),
		slog.Duration("elapsed", elapsed),
	)

	if err != nil {
		return nil, ClassifyTimeoutError(fmt.Errorf("request %s %s failed: %w", method, fullURL, err))
	}

	return resp, nil
}

// Get sends a GET request.
func (c *Client) Get(ctx context.Context, path string, query url.Values, headers map[string]string) (*http.Response, error) {
	fullURL, err := c.resolveURL(path)
	if err != nil {
		return nil, err
	}
	if len(query) > 0 {
		u, err := url.Parse(fullURL)
		if err != nil {
			return nil, err
		}
		existing := u.Query()
		for k, vv := range query {
			for _, v := range vv {
				existing.Add(k, v)
			}
		}
		u.RawQuery = existing.Encode()
		fullURL = u.String()
	}
	return c.Request(ctx, http.MethodGet, fullURL, nil, headers)
}

// Post sends a POST request.
func (c *Client) Post(ctx context.Context, path string, body any, headers map[string]string) (*http.Response, error) {
	return c.Request(ctx, http.MethodPost, path, body, headers)
}

// Put sends a PUT request.
func (c *Client) Put(ctx context.Context, path string, body any, headers map[string]string) (*http.Response, error) {
	return c.Request(ctx, http.MethodPut, path, body, headers)
}

// Delete sends a DELETE request.
func (c *Client) Delete(ctx context.Context, path string, headers map[string]string) (*http.Response, error) {
	return c.Request(ctx, http.MethodDelete, path, nil, headers)
}
