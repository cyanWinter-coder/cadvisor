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
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUID623_ConcurrencyLimiterTryAcquire(t *testing.T) {
	l := NewConcurrencyLimiter(2)
	assert.Equal(t, 0, l.Active())
	assert.True(t, l.TryAcquire())
	assert.True(t, l.TryAcquire())
	assert.Equal(t, 2, l.Active())
	assert.False(t, l.TryAcquire())
	l.Release()
	assert.Equal(t, 1, l.Active())
	assert.True(t, l.TryAcquire())
}

func TestUID623_UnlimitedLimiter(t *testing.T) {
	l := NewConcurrencyLimiter(0)
	for i := 0; i < 100; i++ {
		assert.True(t, l.TryAcquire())
	}
	assert.Equal(t, 0, l.Active())
}

// TestGuardHealthzIndependentOfQueue proves requirement 3: while every
// business concurrency slot is occupied by a slow query, /healthz is still
// served promptly and successfully.
func TestUID623_GuardHealthzIndependentOfQueue(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/v2.0/stats", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("big"))
	})

	metrics := NewRequestMetrics()
	h := Guard(mux, GuardConfig{
		Metrics:        metrics,
		Timeout:        time.Minute,
		MaxConcurrency: 1,
		SkipPaths:      []string{"/healthz"},
	})

	blockDone := make(chan struct{})
	go func() {
		defer close(blockDone)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v2.0/stats", nil))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking query did not acquire its slot")
	}

	start := time.Now()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
	assert.Less(t, elapsed, 200*time.Millisecond, "health probe must not queue behind business queries")
	assert.Equal(t, int64(1), metrics.Count("/healthz"), "health checks are still counted by path")

	close(release)
	<-blockDone
}

// TestGuardQueuedRequestTimesOut proves a business request waiting for a slot
// is bounded by the request timeout and returns an explicit status instead of
// hanging indefinitely.
func TestUID623_GuardQueuedRequestTimesOut(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2.0/stats", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	})

	h := Guard(mux, GuardConfig{
		Timeout:        40 * time.Millisecond,
		MaxConcurrency: 1,
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v2.0/stats", nil))
	}()
	<-entered

	secondRec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(secondRec, httptest.NewRequest(http.MethodGet, "/api/v2.0/stats", nil))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("queued request hung")
	}
	assert.Equal(t, http.StatusGatewayTimeout, secondRec.Code)

	close(release)
	<-firstDone
}
