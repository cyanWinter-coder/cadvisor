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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUID623_TimeoutFastHandlerPassesThrough(t *testing.T) {
	h := TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test", "yes")
		_, _ = w.Write([]byte("hello"))
	}), time.Second)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2.0/version", nil)
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "hello", rec.Body.String())
	assert.Equal(t, "yes", rec.Header().Get("X-Test"))
}

func TestUID623_TimeoutReturnsExplicitStatus(t *testing.T) {
	entered := make(chan struct{})
	h := TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done() // released by cancellation at the deadline
	}), 40*time.Millisecond)

	start := time.Now()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2.0/stats", nil)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler never entered")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("request hung past timeout")
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 500*time.Millisecond, "should return promptly at deadline")

	assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	assert.True(t, strings.Contains(rec.Body.String(), "timed out"), rec.Body.String())
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
}

func TestUID623_TimeoutConfigurableStatusCode(t *testing.T) {
	h := TimeoutHandlerWithOptions(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }),
		TimeoutOptions{Timeout: 20 * time.Millisecond, StatusCode: http.StatusServiceUnavailable},
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestUID623_TimeoutDoesNotOverrideStartedResponse(t *testing.T) {
	finished := make(chan struct{})
	h := TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		close(finished)
		<-r.Context().Done()
	}), 30*time.Millisecond)

	rec := httptest.NewRecorder()
	require.NotPanics(t, func() {
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	})
	<-finished
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "partial", rec.Body.String())
}
