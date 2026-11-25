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
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// DefaultSlowThreshold is the duration above which a request is considered
// slow and emitted as a warning log.
const DefaultSlowThreshold = time.Second

// DefaultMaxSlowSamples bounds the in-memory ring of recent slow requests.
const DefaultMaxSlowSamples = 100

// PathMetrics is an immutable snapshot of counters for one path/route.
type PathMetrics struct {
	Path          string        `json:"path"`
	Count         int64         `json:"count"`
	ErrorCount    int64         `json:"error_count"`
	SlowCount     int64         `json:"slow_count"`
	TotalDuration time.Duration `json:"total_duration_ns"`
	MaxDuration   time.Duration `json:"max_duration_ns"`
	LastDuration  time.Duration `json:"last_duration_ns"`
}

// AverageDuration returns the mean handled request duration.
func (p PathMetrics) AverageDuration() time.Duration {
	if p.Count == 0 {
		return 0
	}
	return p.TotalDuration / time.Duration(p.Count)
}

// SlowRequest describes one request whose handling time crossed the slow
// threshold.
type SlowRequest struct {
	Path     string        `json:"path"`
	Method   string        `json:"method"`
	Status   int           `json:"status"`
	Duration time.Duration `json:"duration_ns"`
	At       time.Time     `json:"at"`
}

// RequestMetrics counts requests by path and records slow request durations.
// It is safe for concurrent use. The zero value is not usable; use
// NewRequestMetrics.
type RequestMetrics struct {
	mu    sync.RWMutex
	paths map[string]*PathMetrics
	slow  []SlowRequest

	// SlowThreshold defines what duration counts as slow. Defaults to
	// DefaultSlowThreshold.
	SlowThreshold time.Duration

	// MaxSlowSamples bounds the retained slow-request ring.
	MaxSlowSamples int

	// PathLabel optionally derives a low-cardinality label from a request
	// (for example the matched route pattern). When nil, r.URL.Path is used.
	PathLabel func(r *http.Request) string

	// SlowHook, when non-nil, is invoked (outside the lock) for every slow
	// request. It is intended for tests and alternate sinks.
	SlowHook func(req SlowRequest)

	now func() time.Time
}

// NewRequestMetrics creates an empty metrics registry with default thresholds.
func NewRequestMetrics() *RequestMetrics {
	return &RequestMetrics{
		paths:          map[string]*PathMetrics{},
		SlowThreshold:  DefaultSlowThreshold,
		MaxSlowSamples: DefaultMaxSlowSamples,
		now:            time.Now,
	}
}

// Record adds one completed request to the registry.
func (m *RequestMetrics) Record(method, path string, status int, duration time.Duration) {
	if path == "" {
		path = "/"
	}
	slowThreshold := m.SlowThreshold
	if slowThreshold <= 0 {
		slowThreshold = DefaultSlowThreshold
	}
	isSlow := duration >= slowThreshold

	var hook func(SlowRequest)
	var sample SlowRequest

	m.mu.Lock()
	p, ok := m.paths[path]
	if !ok {
		p = &PathMetrics{Path: path}
		m.paths[path] = p
	}
	p.Count++
	p.LastDuration = duration
	p.TotalDuration += duration
	if duration > p.MaxDuration {
		p.MaxDuration = duration
	}
	if status >= 500 {
		p.ErrorCount++
	}
	if isSlow {
		p.SlowCount++

		capacity := m.MaxSlowSamples
		if capacity <= 0 {
			capacity = DefaultMaxSlowSamples
		}
		sample = SlowRequest{
			Path:     path,
			Method:   method,
			Status:   status,
			Duration: duration,
			At:       m.currentTimeLocked(),
		}
		m.slow = append(m.slow, sample)
		if len(m.slow) > capacity {
			m.slow = m.slow[len(m.slow)-capacity:]
		}
		hook = m.SlowHook
	}
	m.mu.Unlock()

	if isSlow {
		klog.Warningf("Slow %s request to %q took %s (status %d)", method, path, duration, status)
		if hook != nil {
			hook(sample)
		}
	}
}

func (m *RequestMetrics) currentTimeLocked() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

// Inc increments the counter for path without duration/status information.
func (m *RequestMetrics) Inc(path string) {
	m.Record("", path, http.StatusOK, 0)
}

// Count returns the number of requests observed for one path/label.
func (m *RequestMetrics) Count(path string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if p, ok := m.paths[path]; ok {
		return p.Count
	}
	return 0
}

// RequestCount is an alias for Count.
func (m *RequestMetrics) RequestCount(path string) int64 { return m.Count(path) }

// Counts returns a path -> request count map.
func (m *RequestMetrics) Counts() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int64, len(m.paths))
	for k, v := range m.paths {
		out[k] = v.Count
	}
	return out
}

// Snapshot returns value copies of all per-path metrics, sorted by path.
func (m *RequestMetrics) Snapshot() []PathMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]PathMetrics, 0, len(m.paths))
	for _, p := range m.paths {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// SlowRequests returns the retained slow-request samples (oldest first).
func (m *RequestMetrics) SlowRequests() []SlowRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SlowRequest, len(m.slow))
	copy(out, m.slow)
	return out
}

func (m *RequestMetrics) label(r *http.Request) string {
	if m.PathLabel != nil {
		if l := m.PathLabel(r); l != "" {
			return l
		}
	}
	return r.URL.Path
}

// statusRecorder captures the response status while preserving optional
// interfaces (Flusher/Hijacker) of the wrapped ResponseWriter.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	bytes       int
}

func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w, status: http.StatusOK}
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.wroteHeader = true
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap exposes the wrapped writer to http.NewResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Flush preserves streaming support.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Middleware wraps an http.Handler with per-path counting and slow-request
// timing. It never changes response behavior.
func (m *RequestMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := newStatusRecorder(w)
		next.ServeHTTP(rec, r)
		m.Record(r.Method, m.label(r), rec.status, time.Since(start))
	})
}

// ServeHTTP exposes the current metrics as JSON:
// {"paths":[...],"slow_requests":[...]}.
func (m *RequestMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	slow := make([]SlowRequest, len(m.slow))
	copy(slow, m.slow)
	m.mu.RUnlock()

	resp := struct {
		Paths        []PathMetrics `json:"paths"`
		SlowRequests []SlowRequest `json:"slow_requests"`
	}{
		Paths:        m.Snapshot(),
		SlowRequests: slow,
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		klog.Errorf("failed to encode request metrics: %v", err)
	}
}

// MetricsHandler returns an http.Handler that serves the metrics snapshot.
func (m *RequestMetrics) MetricsHandler() http.Handler { return m }
