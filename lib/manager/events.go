// Copyright 2024 Google Inc. All Rights Reserved.
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

package manager

import (
	"time"

	info "github.com/google/cadvisor/lib/model"

	"k8s.io/klog/v2"
)

// EventSink receives the container lifecycle (creation/deletion) and OOM events
// the manager emits. The full cAdvisor binary injects its events.EventManager
// here (it satisfies this interface) and serves them over the /events REST API;
// the kubelet leaves it nil, so the manager emits nothing and the library
// carries no event-storage machinery. This is the same injection pattern as the
// perf/resctrl factories — see plugins.go.
type EventSink interface {
	AddEvent(*info.Event) error
}

// SetEventSink wires an event sink. Safe to call once before Start; passing nil
// (the default) disables event emission.
func (m *manager) SetEventSink(sink EventSink) {
	m.eventSink = sink
}

// addEvent delivers an event to the sink, best-effort. It is a no-op when no
// sink is wired (the kubelet case).
func (m *manager) addEvent(e *info.Event) {
	if m.eventSink == nil {
		return
	}
	if err := m.eventSink.AddEvent(e); err != nil {
		klog.Errorf("failed to add %s event for %q: %v", e.EventType, e.ContainerName, err)
	}
}

// EventStorageAgeLimit and EventStorageEventLimit expose the values of the
// event_storage_* flags this package registers (for kubelet flag-compatibility)
// so the root binary can build its events storage policy without re-registering
// — and double-registering — the same flags.
func EventStorageAgeLimit() string   { return *eventStorageAgeLimit }
func EventStorageEventLimit() string { return *eventStorageEventLimit }

// ContainerResolver maps cgroup paths to the container instances that owned
// them. The manager implements it; the full binary uses it to attribute kernel
// events (which name cgroup paths) to the right container incarnation. It is a
// separate interface so that Manager stays unchanged for other implementations.
type ContainerResolver interface {
	// ResolveContainer maps a cgroup path seen at a given time (typically one
	// printed in a kernel message) to the container instance that owned it then.
	// Unlike name-based lookups it still works after the container exited and its
	// name or runtime ID was reused. See identity.go.
	ResolveContainer(cgroupPath string, at time.Time) (ContainerResolution, bool)

	// ContainerIdentity returns the identity of the newest incarnation of the
	// named container that has not exited. See identity.go.
	ContainerIdentity(name string) (string, bool)
}

var _ ContainerResolver = &manager{}

// ResolveContainer maps a cgroup path (as printed in a kernel message) to the
// container instance that owned it at the given time. It returns false when no
// tracked container ever owned the path.
func (m *manager) ResolveContainer(cgroupPath string, at time.Time) (ContainerResolution, bool) {
	return m.identityIndex().resolve(cgroupPath, at)
}

// ContainerIdentity returns the identity of the newest incarnation of the named
// container that has not exited. It is used to associate counters with a
// concrete container instance rather than a name that may be reused.
func (m *manager) ContainerIdentity(name string) (string, bool) {
	return m.identityIndex().currentIdentity(name)
}

// identityIndex returns the incarnation index, creating it on first use so that
// managers built directly (tests) still track identities.
func (m *manager) identityIndex() *incarnationIndex {
	m.incarnationsOnce.Do(func() {
		if m.incarnations == nil {
			m.incarnations = newIncarnationIndex(defaultIncarnationRetention)
		}
	})
	return m.incarnations
}
