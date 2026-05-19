package utils

import (
	"context"
	"fmt"
	"io"
	"time"
)

// DefaultStreamIdleTimeout is the default maximum time to wait between
// successive data chunks during SSE streaming. If the provider stops
// sending data for longer than this, the read is aborted.
const DefaultStreamIdleTimeout = 90 * time.Second

// ErrStreamIdleTimeout is returned when no data is received from the
// stream within the configured idle timeout period.
var ErrStreamIdleTimeout = fmt.Errorf("stream idle timeout: no data received within deadline")

// IdleTimeoutReader wraps an io.Reader with per-read idle timeout detection.
// If a Read() call blocks for longer than the configured timeout, it returns
// an error. This prevents goroutines from hanging indefinitely when a provider
// stops sending data mid-stream.
//
// The implementation uses a background goroutine per Read() call with context
// cancellation to ensure clean termination.
type IdleTimeoutReader struct {
	reader      io.Reader
	ctx         context.Context
	idleTimeout time.Duration
}

// NewIdleTimeoutReader creates a reader that aborts if any single Read()
// blocks for longer than idleTimeout. If idleTimeout <= 0, it defaults to
// DefaultStreamIdleTimeout.
func NewIdleTimeoutReader(ctx context.Context, reader io.Reader, idleTimeout time.Duration) *IdleTimeoutReader {
	if idleTimeout <= 0 {
		idleTimeout = DefaultStreamIdleTimeout
	}
	return &IdleTimeoutReader{
		reader:      reader,
		ctx:         ctx,
		idleTimeout: idleTimeout,
	}
}

// Read implements io.Reader with idle timeout detection.
// Each Read() call is bounded by both the parent context and the idle timeout.
func (r *IdleTimeoutReader) Read(p []byte) (int, error) {
	// Fast path: check context before starting
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}

	type readResult struct {
		n   int
		err error
	}

	resultCh := make(chan readResult, 1)

	// Perform the actual read in a goroutine so we can race it against timeout
	go func() {
		n, err := r.reader.Read(p)
		resultCh <- readResult{n: n, err: err}
	}()

	// Create a per-read deadline
	timer := time.NewTimer(r.idleTimeout)
	defer timer.Stop()

	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-timer.C:
		return 0, ErrStreamIdleTimeout
	case result := <-resultCh:
		return result.n, result.err
	}
}
