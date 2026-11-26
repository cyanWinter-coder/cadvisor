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

package api

import (
	"testing"
	"time"

	"github.com/google/cadvisor/events"
	"github.com/google/cadvisor/lib/manager"
	info "github.com/google/cadvisor/lib/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resolverManager is a manager.Manager whose only real behaviour is resolving
// cgroup paths to container incarnations.
type resolverManager struct {
	manager.Manager
	manager.ContainerResolver
	resolve func(cgroupPath string, at time.Time) (manager.ContainerResolution, bool)
}

func (m *resolverManager) ResolveContainer(cgroupPath string, at time.Time) (manager.ContainerResolution, bool) {
	return m.resolve(cgroupPath, at)
}

func (m *resolverManager) ContainerIdentity(name string) (string, bool) { return "", false }

func TestResolveOOMContainerUsesEventTime(t *testing.T) {
	base := time.Now()
	var gotPath string
	var gotTime time.Time
	m := &resolverManager{resolve: func(p string, at time.Time) (manager.ContainerResolution, bool) {
		gotPath, gotTime = p, at
		return manager.ContainerResolution{Name: "/docker/abc", Id: "abc", Identity: "abc#7"}, true
	}}

	name, identity := resolveOOMContainer(m, "/docker/abc", base)
	assert.Equal(t, "/docker/abc", name)
	assert.Equal(t, "abc#7", identity)
	assert.Equal(t, "/docker/abc", gotPath)
	assert.Equal(t, base, gotTime, "the event timestamp must drive the resolution")
}

func TestResolveOOMContainerFallsBackToKernelPath(t *testing.T) {
	m := &resolverManager{resolve: func(string, time.Time) (manager.ContainerResolution, bool) {
		return manager.ContainerResolution{}, false
	}}
	name, identity := resolveOOMContainer(m, "/kubepods/pod-1/ctr-2", time.Now())
	assert.Equal(t, "/kubepods/pod-1/ctr-2", name)
	assert.Empty(t, identity, "an unresolved event carries no identity so it is never aggregated by guess")
}

func TestResolveOOMContainerWithoutManager(t *testing.T) {
	name, identity := resolveOOMContainer(nil, "/docker/abc", time.Now())
	assert.Equal(t, "/docker/abc", name)
	assert.Empty(t, identity)
}

func TestStreamGapEventTypeIsRequestable(t *testing.T) {
	r := makeHTTPRequest("http://localhost:8080/api/v1.3/events?stream_gap_events=true", t)
	query, _, err := getEventRequest(r)
	require.NoError(t, err)
	assert.True(t, query.EventType[info.EventStreamGap])
}

func TestAllEventsIncludesStreamGap(t *testing.T) {
	r := makeHTTPRequest("http://localhost:8080/api/v1.3/events?all_events=true", t)
	query, _, err := getEventRequest(r)
	require.NoError(t, err)
	assert.True(t, query.EventType[info.EventStreamGap])
}

func TestEventManagerRecordsStreamGaps(t *testing.T) {
	em := events.NewEventManager(events.DefaultStoragePolicy())
	recorder, ok := em.(StreamGapRecorder)
	require.True(t, ok, "the event manager must accept stream gap records")

	base := time.Now()
	require.NoError(t, recorder.AddStreamGap(base, base.Add(time.Minute)))

	request := events.NewRequest()
	request.EventType[info.EventStreamGap] = true
	request.MaxEventsReturned = -1
	got, err := em.GetEvents(request)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, info.EventStreamGap, got[0].EventType)
}
