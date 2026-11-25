// Copyright 2026 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultRequestTimeout bounds a single API request. It is deliberately
// shorter than a typical front-end/probe retry window so cAdvisor answers with
// an explicit error instead of pinning goroutines and CPU on tens-of-seconds
// container fan-outs.
const DefaultRequestTimeout = 30 * time.Second

// DefaultTimeoutStatusCode is returned when a request exceeds its deadline.
// 504 (Gateway Timeout) is the explicit, retryable status for a deadline
// exceeded while serving a query.
const DefaultTimeoutStatusCode = http.StatusGatewayTimeout

// TimeoutOptions configures the timeout middleware.
type TimeoutOptions struct {
	// Timeout is the per-request deadline. Zero selects DefaultRequestTimeout.
	Timeout time.Duration

	// StatusCode is written when the deadline expires before the handler
	// started its response. Zero selects DefaultTimeoutStatusCode.
	StatusCode int

	// Message is the JSON "error" value in the timeout response.
	Message string
}

// timeoutWriter synchronizes the handler and the timeout path so a late
// handler write can never race with or overwrite the explicit timeout
// response.
type timeoutWriter struct {
	mu         sync.Mutex
	w          http.ResponseWriter
	timedOut   bool
	responding bool
}

func newTimeoutWriter(w http.ResponseWriter) *timeoutWriter {
	return &timeoutWriter{w: w}
}

// Header is intentionally unsynchronized: handlers set headers before the
// first write, and the timeout response only writes after taking the lock in
// failTimeout, which happens-before its header mutation under the same lock.
func (tw *timeoutWriter) Header() http.Header { return tw.w.Header() }

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut || tw.responding {
		return
	}
	tw.responding = true
	tw.w.WriteHeader(code)
}

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		// The client already received the timeout response; tell the handler
		// its write was abandoned.
		return 0, http.ErrHandlerTimeout
	}
	if !tw.responding {
		tw.responding = true
		tw.w.WriteHeader(http.StatusOK)
	}
	return tw.w.Write(p)
}

func (tw *timeoutWriter) Flush() {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		return
	}
	if f, ok := tw.w.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the wrapped writer to http.NewResponseController.
func (tw *timeoutWriter) Unwrap() http.ResponseWriter { return tw.w }

// failTimeout marks the writer as timed out and writes the explicit timeout
// response unless the handler already started a response. It reports whether
// the timeout response was sent. Marking timedOut even when the response
// already started discards late handler writes and keeps the wrapped recorder
// free of concurrent writes once this middleware returns.
func (tw *timeoutWriter) failTimeout(code int, payload []byte, contentType string) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	started := tw.responding
	tw.timedOut = true
	if started {
		return false
	}
	tw.responding = true
	h := tw.w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.Itoa(len(payload)))
	tw.w.WriteHeader(code)
	_, _ = tw.w.Write(payload)
	return true
}

// TimeoutHandler wraps next so every request is bounded by timeout. When the
// deadline expires before the handler writes a response, an explicit status
// code (DefaultTimeoutStatusCode by default) and a JSON error body are
// returned instead of leaving the client hanging.
//
// The handler receives a request whose context is cancelled at the deadline;
// handlers that honor context abort promptly. Work that ignores context runs
// to completion in the background but its writes are discarded.
func TimeoutHandler(next http.Handler, timeout time.Duration) http.Handler {
	return TimeoutHandlerWithOptions(next, TimeoutOptions{Timeout: timeout})
}

// WithTimeout is an alias for TimeoutHandler.
func WithTimeout(next http.Handler, timeout time.Duration) http.Handler {
	return TimeoutHandler(next, timeout)
}

// TimeoutMiddleware returns a middleware applying TimeoutHandler with d.
func TimeoutMiddleware(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return TimeoutHandler(next, d)
	}
}

// TimeoutHandlerWithOptions is the configurable form of TimeoutHandler.
func TimeoutHandlerWithOptions(next http.Handler, opts TimeoutOptions) http.Handler {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultRequestTimeout
	}
	if opts.StatusCode == 0 {
		opts.StatusCode = DefaultTimeoutStatusCode
	}
	if opts.Message == "" {
		opts.Message = "request timed out"
	}
	body, _ := json.Marshal(map[string]string{"error": opts.Message})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), opts.Timeout)
		defer cancel()

		tw := newTimeoutWriter(w)
		done := make(chan struct{})
		go func() {
			defer close(done)
			next.ServeHTTP(tw, r.WithContext(ctx))
		}()

		select {
		case <-done:
			return
		case <-ctx.Done():
			// Deadline exceeded (or the client went away). Only write the
			// explicit error if we still own the response.
			if r.Context().Err() == nil {
				tw.failTimeout(opts.StatusCode, body, "application/json")
			}
			return
		}
	})
}
