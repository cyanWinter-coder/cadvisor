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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func uid623WaitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

func TestUID623_CoalescerMergesConcurrentCalls(t *testing.T) {
	c := NewCoalescer(0)
	var calls int32
	release := make(chan struct{})

	fn := func() (any, error) {
		atomic.AddInt32(&calls, 1)
		<-release
		return "same-result", nil
	}

	const n = 6
	var wg sync.WaitGroup
	results := make([]any, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.Do("k", fn)
			assert.NoError(t, err)
			results[i] = v
		}(i)
	}

	uid623WaitFor(t, func() bool { return c.InFlight() == 1 && c.Waiters() == n-1 }, "all callers should join one call")
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	for _, v := range results {
		assert.Equal(t, "same-result", v)
	}
}

func TestUID623_CoalescerTTLCache(t *testing.T) {
	now := time.Unix(1000, 0)
	c := NewCoalescer(time.Second)
	c.now = func() time.Time { return now }

	var calls int32
	fn := func() (any, error) {
		atomic.AddInt32(&calls, 1)
		return "v", nil
	}

	v, err := c.Do("k", fn)
	require.NoError(t, err)
	assert.Equal(t, "v", v)

	r2, err := c.Do("k", fn)
	require.NoError(t, err)
	assert.Equal(t, "v", r2)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "fresh TTL entry should not re-run fn")

	now = now.Add(2 * time.Second)
	_, err = c.Do("k", fn)
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls), "expired entry should re-run fn")
}

func TestUID623_CoalescerDoesNotCacheErrorsOrErrorHTTP(t *testing.T) {
	c := NewCoalescer(time.Minute)
	var calls int32
	fn := func() (any, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return nil, assert.AnError
		}
		return "ok", nil
	}
	_, err := c.Do("k", fn)
	assert.Error(t, err)
	_, err = c.Do("k", fn)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), calls)

	assert.False(t, (&coalescedResponse{status: 500}).Cacheable())
	assert.False(t, (&coalescedResponse{status: 200, contentType: "text/plain"}).Cacheable())
	assert.True(t, (&coalescedResponse{status: 200, contentType: "application/json"}).Cacheable())
}

func TestUID623_CoalesceMiddlewareMergesIdenticalRequests(t *testing.T) {
	c := NewCoalescer(time.Second)
	var calls int32
	release := make(chan struct{})
	h := CoalesceMiddleware(CoalesceOptions{Coalescer: c})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	const n = 4
	var wg sync.WaitGroup
	recs := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		recs[i] = httptest.NewRecorder()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.ServeHTTP(recs[i], httptest.NewRequest(http.MethodGet, "/api/v2.0/stats?recursive=true", nil))
		}(i)
	}
	uid623WaitFor(t, func() bool { return c.Waiters() == n-1 }, "identical requests should coalesce")
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	for _, rec := range recs {
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.JSONEq(t, `{"ok":true}`, rec.Body.String())
	}

	// Immediate retry is served from the short TTL cache without a new query.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v2.0/stats?recursive=true", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))

	// A different query is a different key.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/api/v2.0/stats?type=docker", nil))
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestUID623_CoalesceMiddlewareSkipsStreams(t *testing.T) {
	c := NewCoalescer(time.Minute)
	var calls int32
	h := CoalesceMiddleware(CoalesceOptions{Coalescer: c})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1.3/events?stream=true", nil))
	}
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
}
