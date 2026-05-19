package utils

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// TimeoutPhase identifies which phase of an HTTP request timed out.
type TimeoutPhase string

const (
	// TimeoutPhaseDial indicates the TCP connection establishment (DNS + connect) timed out.
	TimeoutPhaseDial TimeoutPhase = "dial"

	// TimeoutPhaseTLS indicates the TLS handshake timed out.
	TimeoutPhaseTLS TimeoutPhase = "tls"

	// TimeoutPhaseHeaderWait indicates waiting for the first response header timed out.
	TimeoutPhaseHeaderWait TimeoutPhase = "header_wait"

	// TimeoutPhaseBodyRead indicates reading the response body timed out (idle timeout).
	TimeoutPhaseBodyRead TimeoutPhase = "body_read"

	// TimeoutPhaseUnknown indicates an unclassified timeout.
	TimeoutPhaseUnknown TimeoutPhase = "unknown"
)

// TimeoutError represents a timeout that occurred during a specific phase
// of an HTTP request. It implements the error interface and can be detected
// via errors.As.
type TimeoutError struct {
	Phase   TimeoutPhase
	Message string
	Err     error
}

func (e *TimeoutError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("http timeout [%s]: %s", e.Phase, e.Message)
	}
	if e.Err != nil {
		return fmt.Sprintf("http timeout [%s]: %v", e.Phase, e.Err)
	}
	return fmt.Sprintf("http timeout [%s]", e.Phase)
}

func (e *TimeoutError) Unwrap() error {
	return e.Err
}

// Timeout implements net.Error interface.
func (e *TimeoutError) Timeout() bool {
	return true
}

// Temporary implements net.Error interface.
func (e *TimeoutError) Temporary() bool {
	return true
}

// ClassifyTimeoutError inspects an error from an HTTP request and wraps it
// as a *TimeoutError with the appropriate phase if it's a timeout error.
// If the error is not a timeout, it returns the original error unchanged.
//
// This allows upstream code to use errors.As to determine which phase timed out:
//
//	var te *utils.TimeoutError
//	if errors.As(err, &te) {
//	    log.Printf("timeout in phase: %s", te.Phase)
//	}
func ClassifyTimeoutError(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's our own stream idle timeout
	if errors.Is(err, ErrStreamIdleTimeout) {
		return &TimeoutError{
			Phase:   TimeoutPhaseBodyRead,
			Message: "stream idle timeout: provider stopped sending data",
			Err:     err,
		}
	}

	// Check for net.Error timeout
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		phase := classifyNetTimeout(err)
		return &TimeoutError{
			Phase: phase,
			Err:   err,
		}
	}

	return err
}

// classifyNetTimeout attempts to determine which phase a net timeout occurred in
// by inspecting the error message. This is heuristic-based since Go's net package
// doesn't expose structured phase information.
func classifyNetTimeout(err error) TimeoutPhase {
	msg := err.Error()

	switch {
	case strings.Contains(msg, "dial"):
		return TimeoutPhaseDial
	case strings.Contains(msg, "TLS handshake"):
		return TimeoutPhaseTLS
	case strings.Contains(msg, "response header"):
		return TimeoutPhaseHeaderWait
	case strings.Contains(msg, "read"):
		return TimeoutPhaseBodyRead
	default:
		return TimeoutPhaseUnknown
	}
}

// IsTimeoutError checks if an error is a timeout error (either our TimeoutError
// or a standard net.Error timeout).
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}
