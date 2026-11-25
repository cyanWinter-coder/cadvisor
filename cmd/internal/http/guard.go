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
	"net/http"
	"strings"
	"time"
)

// ConcurrencyLimiter bounds the number of requests actually executing the
// protected handler. Requests wait for a free slot, respecting their
// (timeout-bound) context. It is the back-pressure valve that keeps retries
// from fanning out into tens of thousands of container queries at once.
//
// Health and other probe routes are expected to bypass the limiter (see
// GuardConfig.SkipPaths) so probe success never depends on query capacity.
type ConcurrencyLimiter struct {
	tokens chan struct{}
	max    int
}

// NewConcurrencyLimiter creates a limiter with max active handlers. A max <= 0
// means unlimited and Middleware passes requests through untouched.
func NewConcurrencyLimiter(max int) *ConcurrencyLimiter {
	l := &ConcurrencyLimiter{max: max}
	if max > 0 {
		l.tokens = make(chan struct{}, max)
	}
	return l
}

// Max returns the configured concurrency bound (0 means unlimited).
func (l *ConcurrencyLimiter) Max() int { return l.max }

// Active returns the number of handlers currently holding a slot.
func (l *ConcurrencyLimiter) Active() int {
	if l.tokens == nil {
		return 0
	}
	return len(l.tokens)
}

// TryAcquire takes a slot if one is immediately available.
func (l *ConcurrencyLimiter) TryAcquire() bool {
	if l.tokens == nil {
		return true
	}
	select {
	case l.tokens <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release returns a slot acquired with Acquire/TryAcquire.
func (l *ConcurrencyLimiter) Release() {
	if l.tokens == nil {
		return
	}
	select {
	case <-l.tokens:
	default:
	}
}

// Middleware applies the bound. If the request context expires while waiting
// for a slot the handler is not invoked; the owning timeout middleware writes
// the explicit status code.
func (l *ConcurrencyLimiter) Middleware(next http.Handler) http.Handler {
	if l == nil || l.tokens == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.tokens <- struct{}{}:
			defer func() { <-l.tokens }()
			next.ServeHTTP(w, r)
		case <-r.Context().Done():
			return
		}
	})
}

// ConcurrencyMiddleware is the functional form of NewConcurrencyLimiter.
func ConcurrencyMiddleware(max int) func(http.Handler) http.Handler {
	return NewConcurrencyLimiter(max).Middleware
}

// GuardConfig declares the HTTP-layer protections to compose.
type GuardConfig struct {
	// Metrics, when set, counts every request by path/label and records slow
	// request durations.
	Metrics *RequestMetrics

	// SlowThreshold optionally overrides Metrics.SlowThreshold.
	SlowThreshold time.Duration

	// Timeout bounds a single protected request. Zero applies
	// DefaultRequestTimeout.
	Timeout time.Duration

	// TimeoutStatusCode optionally overrides the timeout response status.
	TimeoutStatusCode int

	// Coalescer optionally merges identical short-lived queries.
	Coalescer *Coalescer

	// CoalesceShould optionally restricts coalescing to selected requests.
	CoalesceShould func(r *http.Request) bool

	// MaxConcurrency bounds concurrently executing protected requests. Zero
	// disables the bound.
	MaxConcurrency int

	// SkipPaths are exact paths served without timeout/coalescing/concurrency
	// protection. This is how /healthz stays independent of query queueing.
	SkipPaths []string

	// SkipPrefixes are path prefixes with the same bypass semantics.
	SkipPrefixes []string

	// SkipRequest optionally bypasses timeout/coalescing/concurrency for
	// selected requests, e.g. long-lived event streams. Metrics still counts.
	SkipRequest func(r *http.Request) bool
}

func (c GuardConfig) shouldSkip(r *http.Request) bool {
	p := r.URL.Path
	for _, path := range c.SkipPaths {
		if p == path {
			return true
		}
	}
	for _, prefix := range c.SkipPrefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	if c.SkipRequest != nil && c.SkipRequest(r) {
		return true
	}
	return false
}

// Guard wraps h with the configured protections in a fixed order:
//
//	metrics -> bypass gate -> coalesce -> timeout -> concurrency -> h
//
// Metrics sits outermost so even bypassed probes and timed-out requests are
// counted; the bypass gate ensures probes never sit behind the coalescer,
// deadline or concurrency queue.
func Guard(h http.Handler, cfg GuardConfig) http.Handler {
	var inner http.Handler = h

	if cfg.MaxConcurrency > 0 {
		inner = NewConcurrencyLimiter(cfg.MaxConcurrency).Middleware(inner)
	}

	timeoutOpts := TimeoutOptions{Timeout: cfg.Timeout, StatusCode: cfg.TimeoutStatusCode}
	inner = TimeoutHandlerWithOptions(inner, timeoutOpts)

	if cfg.Coalescer != nil {
		inner = CoalesceMiddleware(CoalesceOptions{
			Coalescer:      cfg.Coalescer,
			ShouldCoalesce: cfg.CoalesceShould,
		})(inner)
	}

	guarded := inner
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.shouldSkip(r) {
			h.ServeHTTP(w, r)
			return
		}
		guarded.ServeHTTP(w, r)
	})

	if cfg.Metrics != nil {
		if cfg.SlowThreshold > 0 {
			cfg.Metrics.SlowThreshold = cfg.SlowThreshold
		}
		return cfg.Metrics.Middleware(handler)
	}
	return handler
}

// RouteLabel normalizes a URL path into a low-cardinality route label suitable
// for per-path counters. cAdvisor mounts one prefix handler at /api/, so the
// matched mux pattern is just "/api/"; keeping /api/<version>/<type> while
// dropping container-name arguments gives useful but bounded cardinality.
func RouteLabel(path string) string {
	switch {
	case path == "":
		return "/"
	case path == "/healthz":
		return "/healthz"
	case path == "/metrics" || strings.HasPrefix(path, "/metrics?"):
		return "/metrics"
	case strings.HasPrefix(path, "/api/"):
		rest := strings.TrimPrefix(path, "/api/")
		segments := strings.Split(rest, "/")
		if len(segments) == 0 || segments[0] == "" {
			return "/api"
		}
		if len(segments) == 1 || segments[1] == "" {
			return "/api/" + segments[0]
		}
		return "/api/" + segments[0] + "/" + segments[1]
	case strings.HasPrefix(path, "/static/"):
		return "/static/*"
	case strings.HasPrefix(path, "/debug/pprof/"):
		return "/debug/pprof/*"
	default:
		return path
	}
}
