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

package events

import (
	"testing"
	"time"

	info "github.com/google/cadvisor/info/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func oomEvent(at time.Time, container, identity string) *info.Event {
	return &info.Event{
		ContainerName:     container,
		ContainerIdentity: identity,
		Timestamp:         at,
		EventType:         info.EventOom,
	}
}

func getOomEvents(t *testing.T, manager EventManager) []*info.Event {
	t.Helper()
	request := NewRequest()
	request.EventType[info.EventOom] = true
	request.MaxEventsReturned = -1
	events, err := manager.GetEvents(request)
	require.NoError(t, err)
	return events
}

func TestRepeatedEventsAreAggregated(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	base := time.Now()

	// Same container, repeated within the window: one stored event with a count
	// and the most recent timestamp.
	for i := 0; i < 5; i++ {
		require.NoError(t, manager.AddEvent(oomEvent(base.Add(time.Duration(i)*time.Second), "/c1", "c1#1")))
	}

	events := getOomEvents(t, manager)
	require.Len(t, events, 1)
	assert.EqualValues(t, 5, events[0].Count)
	assert.Equal(t, base, events[0].Timestamp)
	assert.Equal(t, base.Add(4*time.Second), events[0].LastTimestamp)
}

func TestAggregationDoesNotCrossWindow(t *testing.T) {
	policy := DefaultStoragePolicy()
	policy.AggregationWindow = time.Minute
	manager := NewEventManager(policy)
	base := time.Now()

	require.NoError(t, manager.AddEvent(oomEvent(base, "/c1", "c1#1")))
	require.NoError(t, manager.AddEvent(oomEvent(base.Add(2*time.Minute), "/c1", "c1#1")))

	events := getOomEvents(t, manager)
	require.Len(t, events, 2)
	assert.EqualValues(t, 1, events[0].Count)
	assert.EqualValues(t, 1, events[1].Count)
}

func TestAggregationSeparatesReusedContainerIdentity(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	base := time.Now()

	// The same name with a new identity is a different container: its events
	// must not be folded into the previous incarnation's event.
	require.NoError(t, manager.AddEvent(oomEvent(base, "/c1", "c1#1")))
	require.NoError(t, manager.AddEvent(oomEvent(base.Add(time.Second), "/c1", "c1#2")))

	events := getOomEvents(t, manager)
	require.Len(t, events, 2)
	assert.Equal(t, "c1#1", events[0].ContainerIdentity)
	assert.Equal(t, "c1#2", events[1].ContainerIdentity)
}

func TestUnresolvedEventsAreNotAggregated(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	base := time.Now()

	// Without an identity we cannot tell a repeated OOM from a reused name, so
	// each occurrence is kept.
	require.NoError(t, manager.AddEvent(oomEvent(base, "/c1", "")))
	require.NoError(t, manager.AddEvent(oomEvent(base.Add(time.Second), "/c1", "")))

	assert.Len(t, getOomEvents(t, manager), 2)
}

func TestAggregationCoversOomKill(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	base := time.Now()

	for i := 0; i < 3; i++ {
		require.NoError(t, manager.AddEvent(&info.Event{
			ContainerName:     "/c1",
			ContainerIdentity: "c1#1",
			Timestamp:         base.Add(time.Duration(i) * time.Second),
			EventType:         info.EventOomKill,
			EventData: info.EventData{
				OomKill: &info.OomKillEventData{Pid: 100 + i, ProcessName: "worker"},
			},
		}))
	}

	request := NewRequest()
	request.EventType[info.EventOomKill] = true
	request.MaxEventsReturned = -1
	events, err := manager.GetEvents(request)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.EqualValues(t, 3, events[0].Count)
	// The aggregated event describes the most recent kill.
	assert.Equal(t, 102, events[0].EventData.OomKill.Pid)
}

func TestStorageLimitDropsOldestAndCounts(t *testing.T) {
	policy := DefaultStoragePolicy()
	// Disable aggregation to exercise the raw storage cap.
	policy.AggregationWindow = 0
	policy.PerTypeMaxNumEvents[info.EventOom] = 3
	manager := NewEventManager(policy)
	base := time.Now()

	for i := 0; i < 10; i++ {
		require.NoError(t, manager.AddEvent(oomEvent(base.Add(time.Duration(i)*time.Second), "/c1", "c1#1")))
	}

	events := getOomEvents(t, manager)
	require.Len(t, events, 3)
	// The oldest entries were dropped and the newest kept.
	assert.Equal(t, base.Add(7*time.Second), events[0].Timestamp)
	assert.Equal(t, base.Add(9*time.Second), events[2].Timestamp)

	stats := manager.(StatsProvider).Stats()
	assert.EqualValues(t, 7, stats.DroppedEvents)
}

func TestDroppedAggregateStopsReceivingOccurrences(t *testing.T) {
	policy := DefaultStoragePolicy()
	policy.PerTypeMaxNumEvents[info.EventOom] = 1
	manager := NewEventManager(policy)
	base := time.Now()

	require.NoError(t, manager.AddEvent(oomEvent(base, "/c1", "c1#1")))
	require.NoError(t, manager.AddEvent(oomEvent(base.Add(time.Second), "/c1", "c1#1")))
	events := getOomEvents(t, manager)
	require.Len(t, events, 1)
	assert.EqualValues(t, 2, events[0].Count)

	// Another container pushes the aggregated event out of the store. A later
	// event for c1 must start a fresh entry rather than fold into the evicted one.
	require.NoError(t, manager.AddEvent(oomEvent(base.Add(2*time.Second), "/c2", "c2#1")))
	require.NoError(t, manager.AddEvent(oomEvent(base.Add(3*time.Second), "/c1", "c1#1")))

	events = getOomEvents(t, manager)
	require.Len(t, events, 1)
	assert.Equal(t, "/c1", events[0].ContainerName)
	assert.EqualValues(t, 1, events[0].Count)
}

func TestStreamGapIsRecorded(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	base := time.Now()

	recorder, ok := manager.(StreamGapRecorder)
	require.True(t, ok)
	require.NoError(t, recorder.AddStreamGap(base, base.Add(30*time.Second)))

	request := NewRequest()
	request.EventType[info.EventStreamGap] = true
	request.MaxEventsReturned = -1
	events, err := manager.GetEvents(request)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, info.EventStreamGap, events[0].EventType)
	assert.Equal(t, base, events[0].EventData.StreamGap.Since)
	assert.Equal(t, base.Add(30*time.Second), events[0].EventData.StreamGap.Until)

	stats := manager.(StatsProvider).Stats()
	assert.EqualValues(t, 1, stats.StreamGaps)
}

func TestStreamGapReachesWatchers(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	request := NewRequest()
	request.EventType[info.EventStreamGap] = true
	watch, err := manager.WatchEvents(request)
	require.NoError(t, err)
	defer manager.StopWatch(watch.GetWatchId())

	base := time.Now()
	require.NoError(t, manager.(StreamGapRecorder).AddStreamGap(base, base.Add(time.Minute)))

	select {
	case event := <-watch.GetChannel():
		assert.Equal(t, info.EventStreamGap, event.EventType)
	case <-time.After(time.Second):
		t.Fatal("stream gap event was not delivered to the watcher")
	}
}

func TestSlowWatcherDoesNotBlockAddEvent(t *testing.T) {
	manager := NewEventManager(DefaultStoragePolicy())
	request := NewRequest()
	request.EventType[info.EventOom] = true
	watch, err := manager.WatchEvents(request)
	require.NoError(t, err)
	defer manager.StopWatch(watch.GetWatchId())

	// The watcher channel holds 10 events and is never drained. AddEvent must
	// not block once it fills up; the overflow is counted instead.
	done := make(chan struct{})
	go func() {
		defer close(done)
		base := time.Now()
		for i := 0; i < 50; i++ {
			_ = manager.AddEvent(oomEvent(base.Add(time.Duration(i)*time.Second), "/c1", "c1#1"))
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("AddEvent blocked on a slow watcher")
	}

	stats := manager.(StatsProvider).Stats()
	assert.EqualValues(t, 40, stats.MissedDelivery)
}
