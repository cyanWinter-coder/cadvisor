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
	"sync"
	"time"

	"github.com/google/cadvisor/events"
	"github.com/google/cadvisor/lib/manager"
	info "github.com/google/cadvisor/lib/model"
	"github.com/google/cadvisor/utils/oomparser"

	"k8s.io/klog/v2"
)

// oomCounts holds the cumulative per-container OOM-kill count. The OOM watcher
// writes it and the prometheus collector reads it through oomInfoProvider to
// emit container_oom_events_total. The lean library manager no longer watches
// for OOMs, so the binary owns this (one manager per process, so a
// package-level value is enough).
//
// The counter is keyed by container identity rather than name: a runtime may
// reuse a container name once it exits, and the new container must not inherit
// the previous one's OOM count.
var oomCounts = struct {
	sync.Mutex
	byIdentity map[string]uint64
}{byIdentity: map[string]uint64{}}

// StreamGapRecorder records intervals during which the kernel event stream was
// disconnected. It is separate from events.EventManager so that existing
// implementations keep satisfying that interface.
type StreamGapRecorder interface {
	AddStreamGap(since, until time.Time) error
}

// startOOMWatcher reads OOM kills from the kernel log via oomparser and (a)
// emits OOM and OOM-kill events to the event manager and (b) maintains the
// per-container OOM counter. It is best-effort: if the kernel log cannot be
// read, OOM events and the metric are disabled with a warning.
func startOOMWatcher(em events.EventManager, m manager.Manager) {
	oomLog, err := oomparser.New()
	if err != nil {
		klog.Warningf("Could not configure a source for OOM detection, disabling OOM events: %v", err)
		return
	}
	outStream := make(chan *oomparser.OomInstance, 10)
	gapStream := make(chan oomparser.StreamGap, 10)
	go oomLog.StreamOoms(outStream, gapStream)

	go func() {
		for gap := range gapStream {
			recorder, ok := em.(StreamGapRecorder)
			if !ok {
				continue
			}
			// The gap is attributed to the root container so it is visible to
			// any query that includes subcontainers.
			if err := recorder.AddStreamGap(gap.Last, gap.Until); err != nil {
				klog.Errorf("failed to record stream gap %v..%v: %v", gap.Last, gap.Until, err)
			}
		}
	}()

	go func() {
		for oomInstance := range outStream {
			// The kernel reports cgroup paths, which may already have been
			// removed (and even reused) by the time the message is read. Resolve
			// them against the container that owned them when the kill happened
			// instead of against whatever holds the path now.
			oomContainer, oomIdentity := resolveOOMContainer(m, oomInstance.ContainerName, oomInstance.TimeOfDeath)
			victimContainer, victimIdentity := resolveOOMContainer(m, oomInstance.VictimContainerName, oomInstance.TimeOfDeath)

			_ = em.AddEvent(&info.Event{
				ContainerName:     oomContainer,
				ContainerIdentity: oomIdentity,
				Timestamp:         oomInstance.TimeOfDeath,
				EventType:         info.EventOom,
			})
			_ = em.AddEvent(&info.Event{
				ContainerName:     victimContainer,
				ContainerIdentity: victimIdentity,
				Timestamp:         oomInstance.TimeOfDeath,
				EventType:         info.EventOomKill,
				EventData: info.EventData{
					OomKill: &info.OomKillEventData{
						Pid:         oomInstance.Pid,
						ProcessName: oomInstance.ProcessName,
						Constraint:  oomInstance.Constraint,
					},
				},
			})

			oomCounts.Lock()
			oomCounts.byIdentity[oomCountKey(oomContainer, oomIdentity)]++
			oomCounts.Unlock()
		}
	}()
}

// oomCountKey returns the key an OOM count is stored under. The identity is
// preferred; the falling back to the container name keeps the count for OOMs
// whose cgroup could not be resolved (they are not aggregated either).
func oomCountKey(name, identity string) string {
	if identity != "" {
		return identity
	}
	return "name:" + name
}

// resolveOOMContainer maps the cgroup path from an OOM message to the container
// name and identity that owned it at time t. Where the container cannot be
// resolved (for example the cgroup predates cAdvisor), it falls back to the
// kernel path with an empty identity, preserving the previous behaviour.
func resolveOOMContainer(m manager.Manager, cgroupPath string, t time.Time) (name, identity string) {
	path := cgroupPath
	if path == "" {
		path = "/"
	}
	resolver, ok := m.(manager.ContainerResolver)
	if !ok {
		return path, ""
	}
	res, ok := resolver.ResolveContainer(path, t)
	if !ok {
		return path, ""
	}
	return res.Name, res.Identity
}

// containerIdentity returns the identity of the named container's current
// incarnation, when the manager supports resolution.
func containerIdentity(m manager.Manager, name string) (string, bool) {
	resolver, ok := m.(manager.ContainerResolver)
	if !ok {
		return "", false
	}
	return resolver.ContainerIdentity(name)
}

// WrapManagerForOOM wraps a manager so the prometheus collector reports the
// per-container OOM-kill count maintained by the OOM watcher. The lean library
// manager always reports zero for OOMEvents, so without this wrapper
// container_oom_events_total would be stuck at zero.
func WrapManagerForOOM(m manager.Manager) manager.Manager {
	return &oomInfoProvider{Manager: m}
}

type oomInfoProvider struct {
	manager.Manager
}

func (p *oomInfoProvider) GetRequestedContainersInfo(containerName string, options info.RequestOptions) (map[string]*info.ContainerInfo, error) {
	infos, err := p.Manager.GetRequestedContainersInfo(containerName, options)
	if err != nil {
		return infos, err
	}
	oomCounts.Lock()
	defer oomCounts.Unlock()
	for _, ci := range infos {
		// Look the count up by identity so a container that reused an exited
		// container's name does not inherit its OOM count. Fall back to the
		// name key for OOMs that could not be resolved to a container.
		var count uint64
		if identity, ok := containerIdentity(p.Manager, ci.Name); ok {
			count = oomCounts.byIdentity[identity]
		}
		count += oomCounts.byIdentity["name:"+ci.Name]
		for _, stats := range ci.Stats {
			stats.OOMEvents = count
		}
	}
	return infos, err
}
