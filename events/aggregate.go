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
	"time"

	info "github.com/google/cadvisor/info/v1"
)

// maxAggregatedKeys bounds how many distinct events the aggregator tracks
// between prunes, so a high event rate cannot grow it without limit.
const maxAggregatedKeys = 100000

// aggregator folds repeated occurrences of the same event into a single stored
// event that keeps the number of occurrences and the most recent timestamp.
// Occurrences are folded when they share an identity key and arrive within the
// aggregation window of the previous occurrence.
//
// The aggregator is not thread-safe; the owning event manager serializes access
// with eventsLock.
type aggregator struct {
	window time.Duration
	// last maps a key to the time of its most recent occurrence.
	last map[aggregateKey]time.Time
	// stored maps a key to the stored event occurrences are folded into.
	stored map[aggregateKey]*info.Event
}

func newAggregator(window time.Duration) *aggregator {
	return &aggregator{
		window: window,
		last:   make(map[aggregateKey]time.Time),
		stored: make(map[aggregateKey]*info.Event),
	}
}

func newAggregateKey(event *info.Event) (aggregateKey, bool) {
	// Aggregation only applies to events that can repeat on their own. Lifecycle
	// events are already unique per container incarnation.
	if event.EventType != info.EventOom && event.EventType != info.EventOomKill {
		return aggregateKey{}, false
	}
	// Without a resolved identity there is no way to tell a repeat occurrence
	// from a reuse of the container name, so such events are never folded.
	if event.ContainerIdentity == "" {
		return aggregateKey{}, false
	}
	return aggregateKey{
		eventType: event.EventType,
		container: event.ContainerName,
		identity:  event.ContainerIdentity,
	}, true
}

// lookup returns the stored event a new occurrence at at should be folded into,
// if any. Only an event still inside the window is eligible.
func (a *aggregator) lookup(key aggregateKey, at time.Time) *info.Event {
	if a.window <= 0 {
		return nil
	}
	last, ok := a.last[key]
	if !ok {
		return nil
	}
	event := a.stored[key]
	if event == nil {
		return nil
	}
	if at.Before(last) || at.Sub(last) > a.window {
		return nil
	}
	return event
}

// remember records that stored is the aggregated event for key and that its
// most recent occurrence was at at.
func (a *aggregator) remember(key aggregateKey, stored *info.Event, at time.Time) {
	if len(a.stored) >= maxAggregatedKeys {
		a.prune(at)
	}
	a.last[key] = at
	a.stored[key] = stored
}

// forget removes any keys whose stored event was dropped from the event store.
func (a *aggregator) forget(dropped []interface{}) {
	for _, item := range dropped {
		event, ok := item.(*info.Event)
		if !ok {
			continue
		}
		key, ok := newAggregateKey(event)
		if !ok {
			continue
		}
		if a.stored[key] == event {
			delete(a.stored, key)
			delete(a.last, key)
		}
	}
}

// forgetKey stops folding occurrences into the event stored for key.
func (a *aggregator) forgetKey(key aggregateKey) {
	delete(a.stored, key)
	delete(a.last, key)
}

// prune drops keys that have not been seen within the window relative to now.
func (a *aggregator) prune(now time.Time) {
	cutoff := now.Add(-a.window)
	for key, last := range a.last {
		if last.Before(cutoff) {
			delete(a.last, key)
			delete(a.stored, key)
		}
	}
}
