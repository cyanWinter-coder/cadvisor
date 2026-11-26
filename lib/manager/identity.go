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

//go:build linux

package manager

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	info "github.com/google/cadvisor/lib/model"
)

// defaultIncarnationRetention is how long the identity index remembers a
// container after it exits. Kernel events (notably OOM kills) are matched
// against it after the fact, and the container may already be gone when the
// event is read, so the history has to outlive the container by a wide margin.
// It matches the default event storage age limit.
const defaultIncarnationRetention = 24 * time.Hour

// maxOpenIncarnations bounds how many exited incarnations the identity index
// remembers between prunes. Container churn is bounded by the retention window,
// but a pathological churn rate should not grow the index without limit.
const maxOpenIncarnations = 100000

// incarnationPruneInterval is how many registrations may pass between prunes.
// Pruning walks the whole index, so doing it on every container creation would
// make registration quadratic under churn.
const incarnationPruneInterval = 256

// ContainerResolution identifies the concrete container incarnation that owned
// a cgroup path at a point in time. Identity is unique per incarnation even if
// the container name or runtime ID is later reused.
type ContainerResolution struct {
	// Name is the absolute cAdvisor container name.
	Name string
	// Id is the runtime container ID, when the runtime provides one.
	Id string
	// Identity is unique to this incarnation of the container.
	Identity string
}

// incarnation is one concrete lifetime of a container name.
type incarnation struct {
	identity    string
	name        string
	id          string
	namespace   string
	cgroupPaths []string
	created     time.Time
	deleted     time.Time
}

// incarnationIndex remembers which container incarnation owned which cgroup
// path, and when. Container names (and the runtime IDs embedded in cgroup
// paths) can be reused once a container exits; keying events by name alone
// would then attribute a late event to the new container. The index keeps the
// create/delete interval of every incarnation so an event can be resolved
// against the container that existed at the event's timestamp.
type incarnationIndex struct {
	lock sync.RWMutex
	// seq makes identities unique even across name and ID reuse.
	seq uint64
	// byName maps a container name to its incarnations, oldest first.
	byName map[string][]*incarnation
	// byCgroup maps normalized cgroup path variants to incarnations.
	byCgroup map[string][]*incarnation
	// byIdentity indexes incarnations by their identity.
	byIdentity map[string]*incarnation
	// retention is how long exited incarnations are remembered.
	retention time.Duration
	// registrations counts registrations since the last prune.
	registrations int
}

func newIncarnationIndex(retention time.Duration) *incarnationIndex {
	return &incarnationIndex{
		byName:     make(map[string][]*incarnation),
		byCgroup:   make(map[string][]*incarnation),
		byIdentity: make(map[string]*incarnation),
		retention:  retention,
	}
}

// register records a new incarnation for the given container reference and
// cgroup paths, and returns its identity. createdAt should be the container's
// own creation time where known, so events that happened before cAdvisor first
// noticed the container still resolve to it.
func (ix *incarnationIndex) register(ref info.ContainerReference, cgroupPaths []string, createdAt, now time.Time) string {
	if createdAt.IsZero() || createdAt.After(now) {
		createdAt = now
	}
	ix.lock.Lock()
	defer ix.lock.Unlock()

	ix.seq++
	inc := &incarnation{
		identity:  fmt.Sprintf("%s#%d", identityBase(ref), ix.seq),
		name:      ref.Name,
		id:        ref.Id,
		namespace: ref.Namespace,
		created:   createdAt,
	}
	inc.cgroupPaths = inc.cgroupKeys(ref.Name, cgroupPaths)

	ix.byName[ref.Name] = append(ix.byName[ref.Name], inc)
	ix.byIdentity[inc.identity] = inc
	for _, key := range inc.cgroupPaths {
		ix.byCgroup[key] = append(ix.byCgroup[key], inc)
	}

	ix.registrations++
	if ix.registrations >= incarnationPruneInterval {
		ix.registrations = 0
		ix.pruneLocked(now)
	}
	return inc.identity
}

// markDeleted records that the incarnation identified by identity exited at t.
func (ix *incarnationIndex) markDeleted(identity string, t time.Time) {
	if identity == "" {
		return
	}
	ix.lock.Lock()
	defer ix.lock.Unlock()
	if inc, ok := ix.byIdentity[identity]; ok && inc.deleted.IsZero() {
		inc.deleted = t
	}
}

// currentIdentity returns the identity of the newest incarnation of name that
// has not exited.
func (ix *incarnationIndex) currentIdentity(name string) (string, bool) {
	ix.lock.RLock()
	defer ix.lock.RUnlock()
	incs := ix.byName[name]
	for i := len(incs) - 1; i >= 0; i-- {
		if incs[i].deleted.IsZero() {
			return incs[i].identity, true
		}
	}
	return "", false
}

// resolve returns the incarnation that most plausibly owned cgroupPath at time
// at. It never returns an incarnation created after at unless no earlier one
// is known, so an event that predates a reused name resolves to the container
// that was actually running then.
func (ix *incarnationIndex) resolve(cgroupPath string, at time.Time) (ContainerResolution, bool) {
	normalized := normalizeCgroupPath(cgroupPath)

	ix.lock.RLock()
	defer ix.lock.RUnlock()

	if inc := pickIncarnation(ix.byCgroup[normalized], at); inc != nil {
		return inc.resolution(), true
	}
	// The kernel prints the cgroup-relative path while a handler may report an
	// absolute filesystem path (or vice versa).
	if stripped := stripCgroupMount(normalized); stripped != normalized {
		if inc := pickIncarnation(ix.byCgroup[stripped], at); inc != nil {
			return inc.resolution(), true
		}
	}

	// A containerized cAdvisor can see a different cgroup root than the one the
	// kernel printed the path for, so fall back to a suffix match. Require at
	// least two components so a bare name does not match unrelated paths.
	for _, candidate := range []string{normalized, stripCgroupMount(normalized)} {
		if strings.Count(candidate, "/") < 2 {
			continue
		}
		var best *incarnation
		for key, incs := range ix.byCgroup {
			if key != candidate && !strings.HasSuffix(key, candidate) {
				continue
			}
			if inc := pickIncarnation(incs, at); inc != nil {
				if best == nil || (inc.created.After(best.created) && !inc.created.After(at)) {
					best = inc
				}
			}
		}
		if best != nil {
			return best.resolution(), true
		}
	}
	return ContainerResolution{}, false
}

func (inc *incarnation) resolution() ContainerResolution {
	return ContainerResolution{Name: inc.name, Id: inc.id, Identity: inc.identity}
}

// pickIncarnation selects the incarnation that was running at at: the latest
// one created at or before at and not yet exited. If none was running then, the
// latest incarnation created before at is the best guess (the container may
// have exited before cAdvisor noticed); failing that, the oldest one (a clock
// skew or a cgroup first seen after the event).
func pickIncarnation(incs []*incarnation, at time.Time) *incarnation {
	if len(incs) == 0 {
		return nil
	}
	var active, latestBefore *incarnation
	for _, inc := range incs {
		if inc.created.After(at) {
			continue
		}
		if latestBefore == nil || inc.created.After(latestBefore.created) {
			latestBefore = inc
		}
		if inc.deleted.IsZero() || !inc.deleted.Before(at) {
			if active == nil || inc.created.After(active.created) {
				active = inc
			}
		}
	}
	if active != nil {
		return active
	}
	if latestBefore != nil {
		return latestBefore
	}
	return incs[0]
}

// pruneLocked forgets incarnations that exited more than retention ago, and
// trims the oldest exited incarnations if the index has grown past its hard
// limit. Incarnations that have not exited are never forgotten.
func (ix *incarnationIndex) pruneLocked(now time.Time) {
	cutoff := now.Add(-ix.retention)
	drop := make(map[*incarnation]struct{})
	for _, incs := range ix.byName {
		for _, inc := range incs {
			if !inc.deleted.IsZero() && inc.deleted.Before(cutoff) {
				drop[inc] = struct{}{}
			}
		}
	}

	// Budget accounting: keep at most maxOpenIncarnations exited incarnations,
	// evicting the oldest first, so a pathological churn rate cannot grow the
	// index without limit. Live incarnations are always kept.
	if excess := len(ix.byIdentity) - len(drop) - maxOpenIncarnations; excess > 0 {
		var exited []*incarnation
		for _, inc := range ix.byIdentity {
			if _, dropped := drop[inc]; dropped || inc.deleted.IsZero() {
				continue
			}
			exited = append(exited, inc)
		}
		sort.Slice(exited, func(i, j int) bool { return exited[i].deleted.Before(exited[j].deleted) })
		for i := 0; i < excess && i < len(exited); i++ {
			drop[exited[i]] = struct{}{}
		}
	}

	if len(drop) == 0 {
		return
	}

	for name, incs := range ix.byName {
		kept := incs[:0]
		for _, inc := range incs {
			if _, dropped := drop[inc]; !dropped {
				kept = append(kept, inc)
			}
		}
		if len(kept) == 0 {
			delete(ix.byName, name)
			continue
		}
		ix.byName[name] = kept
	}
	for key, incs := range ix.byCgroup {
		kept := incs[:0]
		for _, inc := range incs {
			if _, dropped := drop[inc]; !dropped {
				kept = append(kept, inc)
			}
		}
		if len(kept) == 0 {
			delete(ix.byCgroup, key)
			continue
		}
		ix.byCgroup[key] = kept
	}
	for inc := range drop {
		delete(ix.byIdentity, inc.identity)
	}
}

func identityBase(ref info.ContainerReference) string {
	if ref.Id != "" && ref.Namespace != "" {
		return ref.Namespace + "/" + ref.Id
	}
	if ref.Id != "" {
		return ref.Id
	}
	return ref.Name
}

// inc.cgroupKeys returns the normalized forms under which a container's cgroup
// paths should match the paths printed in kernel messages. The cAdvisor
// container name is itself the cgroup-relative path, and handler cgroup paths
// are absolute filesystem paths, possibly rooted at a host mount.
func (inc *incarnation) cgroupKeys(name string, cgroupPaths []string) []string {
	seen := make(map[string]struct{}, len(cgroupPaths)+2)
	out := make([]string, 0, len(cgroupPaths)+2)
	add := func(p string) {
		p = normalizeCgroupPath(p)
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	add(name)
	for _, p := range cgroupPaths {
		add(p)
		add(stripCgroupMount(p))
	}
	return out
}

// stripCgroupMount removes a cgroup filesystem prefix (and the controller
// directory on cgroup v1) from an absolute cgroup path.
func stripCgroupMount(p string) string {
	const mount = "/sys/fs/cgroup"
	if p == mount {
		return "/"
	}
	if !strings.HasPrefix(p, mount+"/") {
		return p
	}
	rel := strings.TrimPrefix(p, mount+"/")
	// On cgroup v1 the path continues with the controller name, e.g.
	// memory/docker/<id>. Drop a leading controller component when there is
	// something after it.
	if i := strings.Index(rel, "/"); i > 0 {
		first := rel[:i]
		if isCgroupController(first) {
			rel = rel[i+1:]
		}
	}
	return "/" + rel
}

func isCgroupController(name string) bool {
	switch name {
	case "memory", "cpu", "cpuacct", "cpuset", "blkio", "pids", "devices",
		"freezer", "net_cls", "net_prio", "perf_event", "hugetlb", "rdma",
		"misc", "unified", "systemd":
		return true
	}
	return false
}

// normalizeCgroupPath makes a cgroup path comparable: absolute, cleaned and
// without a trailing slash.
func normalizeCgroupPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	return path.Clean("/" + strings.TrimPrefix(p, "/"))
}
