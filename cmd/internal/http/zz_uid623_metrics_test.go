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
	"github.com/stretchr/testify/require"
)

func TestUID623_RequestMetricsCountsByPath(t *testing.T) {
	m := NewRequestMetrics()
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v2.0/version", nil))
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))

	assert.Equal(t, int64(3), m.Count("/api/v2.0/version"))
	assert.Equal(t, int64(1), m.Count("/boom"))
	assert.Equal(t, int64(0), m.Count("/missing"))

	counts := m.Counts()
	assert.Equal(t, int64(3), counts["/api/v2.0/version"])

	var boom PathMetrics
	for _, pm := range m.Snapshot() {
		if pm.Path == "/boom" {
			boom = pm
		}
	}
	assert.Equal(t, int64(1), boom.ErrorCount)
}

func TestUID623_RequestMetricsSlowRequestsRecorded(t *testing.T) {
	m := NewRequestMetrics()
	m.SlowThreshold = 5 * time.Millisecond
	hooked := make(chan SlowRequest, 1)
	m.SlowHook = func(sr SlowRequest) { hooked <- sr }

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/slow", nil))

	slow := m.SlowRequests()
	require.Len(t, slow, 1)
	assert.Equal(t, "/slow", slow[0].Path)
	assert.GreaterOrEqual(t, slow[0].Duration, 10*time.Millisecond)
	assert.Equal(t, http.StatusOK, slow[0].Status)

	select {
	case sr := <-hooked:
		assert.Equal(t, "/slow", sr.Path)
	case <-time.After(time.Second):
		t.Fatal("slow hook not invoked")
	}

	var pm PathMetrics
	for _, p := range m.Snapshot() {
		if p.Path == "/slow" {
			pm = p
		}
	}
	assert.Equal(t, int64(1), pm.SlowCount)
	assert.GreaterOrEqual(t, pm.MaxDuration, 10*time.Millisecond)
}

func TestUID623_RequestMetricsServesJSON(t *testing.T) {
	m := NewRequestMetrics()
	m.Record("GET", "/api/v2.0/machine", 200, time.Millisecond)
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/http-requests", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/api/v2.0/machine")
	assert.Contains(t, rec.Body.String(), `"count":1`)
}

func TestUID623_RouteLabel(t *testing.T) {
	cases := map[string]string{
		"/healthz":                           "/healthz",
		"/api/":                              "/api",
		"/api/v2.0/stats":                    "/api/v2.0/stats",
		"/api/v2.0/stats/docker/abc123":      "/api/v2.0/stats",
		"/api/v1.3/subcontainers/docker/x/y": "/api/v1.3/subcontainers",
		"/metrics":                           "/metrics",
		"/static/foo/bar.css":                "/static/*",
	}
	for in, want := range cases {
		assert.Equalf(t, want, RouteLabel(in), "input %s", in)
	}
}
