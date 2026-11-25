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
	"net/http"
	"strings"
	"sync"
	"time"
)

// Coalescer merges identical queries that arrive close together.
//
// Two mechanisms are provided:
//
//   - Single-flight: concurrent calls with the same key execute fn only once;
//     every caller receives the same result.
//   - Short TTL cache: a successful result is reused for ttl, so a front end
//     that times out and retries the same query immediately does not trigger
//     another expensive fan-out across thousands of containers.
//
// It is safe for concurrent use. The zero value must not be used; construct
// one with NewCoalescer.
type Coalescer struct {
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	calls map[string]*inflightCall
	cache map[string]cachedResult
	// waiters is the number of goroutines blocked joining an in-flight call.
	waiters int
}

type cachedResult struct {
	value any
	err   error
	at    time.Time
}

type inflightCall struct {
	done chan struct{}
	res  result
}

type result struct {
	value any
	err   error
}

// Result describes the outcome of a Coalescer.DoContext call.
type Result struct {
	// Value is the shared function result.
	Value any
	// Err is the shared function error.
	Err error
	// Shared is true when another in-flight call produced the result.
	Shared bool
	// Cached is true when the result came from a still-fresh TTL entry.
	Cached bool
}

// NewCoalescer creates a Coalescer caching successful results for ttl. A ttl
// <= 0 disables the TTL cache while keeping concurrent single-flight merging.
func NewCoalescer(ttl time.Duration) *Coalescer {
	return &Coalescer{
		ttl:   ttl,
		now:   time.Now,
		calls: map[string]*inflightCall{},
		cache: map[string]cachedResult{},
	}
}

// TTL returns the configured cache duration.
func (c *Coalescer) TTL() time.Duration { return c.ttl }

// Do merges identical concurrent calls and reuses fresh results.
func (c *Coalescer) Do(key string, fn func() (any, error)) (any, error) {
	r := c.DoContext(context.Background(), key, func(context.Context) (any, error) {
		return fn()
	})
	return r.Value, r.Err
}

// DoContext is the context-aware form of Do. Waiting callers abort when ctx
// is done, but the shared leader execution is not tied to any single caller.
func (c *Coalescer) DoContext(ctx context.Context, key string, fn func(context.Context) (any, error)) Result {
	// 1. Fresh TTL entry?
	if c.ttl > 0 {
		c.mu.Lock()
		if entry, ok := c.cache[key]; ok && c.now().Sub(entry.at) < c.ttl {
			c.mu.Unlock()
			return Result{Value: entry.value, Err: entry.err, Cached: true, Shared: true}
		}
		c.mu.Unlock()
	}

	// 2. Join an in-flight call or become its leader.
	c.mu.Lock()
	if call, ok := c.calls[key]; ok {
		c.waiters++
		c.mu.Unlock()
		defer func() {
			c.mu.Lock()
			c.waiters--
			c.mu.Unlock()
		}()
		select {
		case <-call.done:
			return Result{Value: call.res.value, Err: call.res.err, Shared: true}
		case <-ctx.Done():
			return Result{Err: ctx.Err(), Shared: true}
		}
	}
	call := &inflightCall{done: make(chan struct{})}
	c.calls[key] = call
	c.mu.Unlock()

	// Leaders run the work without holding the lock.
	value, err := fn(ctx)
	call.res = result{value: value, err: err}
	close(call.done)

	c.mu.Lock()
	delete(c.calls, key)
	if err == nil && c.ttl > 0 && isCacheable(value) {
		c.cache[key] = cachedResult{value: value, at: c.now()}
	}
	c.mu.Unlock()

	return Result{Value: value, Err: err}
}

// cacheableResult lets a shared value opt out of TTL caching (for example an
// HTTP error response) while still benefiting from in-flight merging.
type cacheableResult interface {
	Cacheable() bool
}

func isCacheable(v any) bool {
	cr, ok := v.(cacheableResult)
	return !ok || cr.Cacheable()
}

// InFlight returns the number of currently running shared calls.
func (c *Coalescer) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

// Waiters returns the number of callers blocked on an in-flight shared call.
func (c *Coalescer) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiters
}

// Cached returns the number of fresh or stale TTL entries currently held.
func (c *Coalescer) Cached() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// Invalidate drops one cached key.
func (c *Coalescer) Invalidate(key string) {
	c.mu.Lock()
	delete(c.cache, key)
	c.mu.Unlock()
}

// Reset clears the TTL cache. In-flight calls are untouched.
func (c *Coalescer) Reset() {
	c.mu.Lock()
	c.cache = map[string]cachedResult{}
	c.mu.Unlock()
}

// SingleflightGroup is an alias naming the common single-flight primitive.
type SingleflightGroup = Coalescer

// NewSingleflightGroup creates a coalescer without TTL caching: only
// concurrent identical calls are merged.
func NewSingleflightGroup() *SingleflightGroup { return NewCoalescer(0) }

// ---- HTTP response coalescing ----

// coalescedResponse is the buffered leader response shared with waiters.
type coalescedResponse struct {
	status      int
	header      http.Header
	body        []byte
	contentType string
}

// CoalesceOptions configures CoalesceMiddleware.
type CoalesceOptions struct {
	// Coalescer provides merging/TTL. Required.
	Coalescer *Coalescer

	// ShouldCoalesce decides whether a request is mergeable. When nil,
	// DefaultShouldCoalesce is used (GET/HEAD JSON API reads, excluding
	// long-lived streams).
	ShouldCoalesce func(r *http.Request) bool

	// Key builds the merge key. When nil, RequestKey is used.
	Key func(r *http.Request) string
}

// DefaultShouldCoalesce merges safe (GET/HEAD) API requests but never event
// streams or other explicitly streaming calls.
func DefaultShouldCoalesce(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	if r.URL.Query().Get("stream") == "true" {
		return false
	}
	return true
}

// RequestKey returns the method, path and query that identify a query.
func RequestKey(r *http.Request) string {
	return r.Method + " " + r.URL.Path + "?" + r.URL.RawQuery
}

// CoalesceMiddleware buffers and merges identical safe HTTP requests for the
// lifetime/TTL of the supplied Coalescer. Only successful JSON responses are
// put into the short TTL cache; in-flight merging shares whatever the leader
// produced.
func CoalesceMiddleware(opts CoalesceOptions) func(http.Handler) http.Handler {
	c := opts.Coalescer
	should := opts.ShouldCoalesce
	if should == nil {
		should = DefaultShouldCoalesce
	}
	keyFn := opts.Key
	if keyFn == nil {
		keyFn = RequestKey
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c == nil || !should(r) {
				next.ServeHTTP(w, r)
				return
			}
			key := keyFn(r)
			res := c.DoContext(r.Context(), key, func(ctx context.Context) (any, error) {
				rec := newBufferedRecorder()
				next.ServeHTTP(rec, r.WithContext(ctx))
				return rec.response(), nil
			})

			if res.Err != nil {
				// A waiting caller gave up (typically its own deadline); the
				// outer timeout middleware owns the client response.
				return
			}
			cr, ok := res.Value.(*coalescedResponse)
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			for k, vs := range cr.header {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			if cr.status != 0 {
				w.WriteHeader(cr.status)
			}
			_, _ = w.Write(cr.body)
		})
	}
}

// bufferedRecorder captures the leader response so it can be replayed.
type bufferedRecorder struct {
	header     http.Header
	status     int
	wroteHead  bool
	body       []byte
	flushCount int
}

func newBufferedRecorder() *bufferedRecorder {
	return &bufferedRecorder{header: http.Header{}, status: http.StatusOK}
}

func (b *bufferedRecorder) Header() http.Header { return b.header }

func (b *bufferedRecorder) WriteHeader(code int) {
	if b.wroteHead {
		return
	}
	b.wroteHead = true
	b.status = code
}

func (b *bufferedRecorder) Write(p []byte) (int, error) {
	b.wroteHead = true
	b.body = append(b.body, p...)
	return len(p), nil
}

// Flush marks streaming; a flushed (streaming) response is not replayable and
// the recorder simply grows. DefaultShouldCoalesce excludes streams, so this
// is only a safety valve.
func (b *bufferedRecorder) Flush() { b.flushCount++ }

func (b *bufferedRecorder) response() *coalescedResponse {
	h := b.header.Clone()
	return &coalescedResponse{status: b.status, header: h, body: b.body, contentType: h.Get("Content-Type")}
}

// Cacheable reports whether the captured HTTP response should enter the short
// TTL cache. Only successful JSON responses are cached; in-flight merging
// still shares every response with concurrent identical callers.
func (c *coalescedResponse) Cacheable() bool {
	if c.status < 200 || c.status >= 300 {
		return false
	}
	return c.contentType == "" || strings.Contains(c.contentType, "application/json")
}
