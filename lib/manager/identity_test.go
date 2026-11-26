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
	"testing"
	"time"

	info "github.com/google/cadvisor/lib/model"

	"github.com/stretchr/testify/assert"
)

const reusedName = "/docker/abc123"

func dockerRef() info.ContainerReference {
	return info.ContainerReference{Name: reusedName, Id: "abc123", Namespace: DockerNamespace}
}

func TestResolveKeepsEventsWithTheirIncarnation(t *testing.T) {
	ix := newIncarnationIndex(time.Hour)
	base := time.Now()

	oldIdentity := ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, base, base)
	ix.markDeleted(oldIdentity, base.Add(10*time.Minute))

	// The runtime reuses the name and the cgroup path for a new container.
	newIdentity := ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, base.Add(20*time.Minute), base.Add(20*time.Minute))
	assert.NotEqual(t, oldIdentity, newIdentity, "identities must be unique across reuse")

	// An event from the first container resolves to the first incarnation even
	// though the path now belongs to the second one.
	res, ok := ix.resolve("/sys/fs/cgroup/docker/abc123", base.Add(5*time.Minute))
	assert.True(t, ok)
	assert.Equal(t, oldIdentity, res.Identity)
	assert.Equal(t, reusedName, res.Name)

	// An event after the reuse resolves to the new incarnation.
	res, ok = ix.resolve("/sys/fs/cgroup/docker/abc123", base.Add(25*time.Minute))
	assert.True(t, ok)
	assert.Equal(t, newIdentity, res.Identity)

	// The exited incarnation is no longer current.
	current, ok := ix.currentIdentity(reusedName)
	assert.True(t, ok)
	assert.Equal(t, newIdentity, current)
}

func TestResolveAfterContainerExit(t *testing.T) {
	ix := newIncarnationIndex(time.Hour)
	base := time.Now()
	identity := ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, base, base)
	ix.markDeleted(identity, base.Add(time.Minute))

	// A late OOM message arrives after the container exited and its cgroup was
	// removed. The event still resolves to the container that was killed.
	res, ok := ix.resolve("/docker/abc123", base.Add(30*time.Second))
	assert.True(t, ok)
	assert.Equal(t, identity, res.Identity)

	// Nothing current remains for the name.
	_, ok = ix.currentIdentity(reusedName)
	assert.False(t, ok)
}

func TestResolveMatchesKernelAndFilesystemPaths(t *testing.T) {
	ix := newIncarnationIndex(time.Hour)
	base := time.Now()
	kernelPath := "/kubepods/burstable/pod-1234/container-5678"

	identity := ix.register(
		info.ContainerReference{Name: kernelPath, Id: "5678"},
		[]string{"/sys/fs/cgroup" + kernelPath},
		base, base)

	for _, p := range []string{
		kernelPath,
		"/sys/fs/cgroup" + kernelPath,
		"/sys/fs/cgroup/memory" + kernelPath, // cgroup v1 controller directory
	} {
		res, ok := ix.resolve(p, base.Add(time.Minute))
		assert.True(t, ok, "path %q should resolve", p)
		assert.Equal(t, identity, res.Identity, "path %q", p)
	}
}

func TestResolveUnknownPath(t *testing.T) {
	ix := newIncarnationIndex(time.Hour)
	base := time.Now()
	ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, base, base)

	_, ok := ix.resolve("/some/other/container", base)
	assert.False(t, ok)
	_, ok = ix.resolve("/", base)
	assert.False(t, ok)
}

func TestResolveUsesContainerCreationTime(t *testing.T) {
	ix := newIncarnationIndex(time.Hour)
	now := time.Now()
	created := now.Add(-2 * time.Hour)

	// cAdvisor first sees the container long after it was created; an event from
	// its early life must still resolve to it.
	identity := ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, created, now)

	res, ok := ix.resolve("/docker/abc123", created.Add(time.Minute))
	assert.True(t, ok)
	assert.Equal(t, identity, res.Identity)
}

func TestPruneForgetsExpiredIncarnations(t *testing.T) {
	ix := newIncarnationIndex(time.Minute)
	base := time.Now()

	old := ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, base, base)
	ix.markDeleted(old, base.Add(time.Second))

	// Pruning after the retention window forgets the long-gone incarnation.
	ix.lock.Lock()
	ix.pruneLocked(base.Add(2 * time.Minute))
	ix.lock.Unlock()

	_, ok := ix.resolve("/docker/abc123", base)
	assert.False(t, ok, "expired incarnation should have been pruned")
}

func TestPruneKeepsLiveIncarnations(t *testing.T) {
	ix := newIncarnationIndex(time.Minute)
	base := time.Now()

	identity := ix.register(dockerRef(), []string{"/sys/fs/cgroup/docker/abc123"}, base, base)

	// A container that has not exited is never forgotten, however old.
	ix.lock.Lock()
	ix.pruneLocked(base.Add(24 * time.Hour))
	ix.lock.Unlock()

	_, ok := ix.resolve("/docker/abc123", base)
	assert.True(t, ok)
	current, ok := ix.currentIdentity(reusedName)
	assert.True(t, ok)
	assert.Equal(t, identity, current)
}

func TestNormalizeCgroupPath(t *testing.T) {
	cases := map[string]string{
		"":                          "/",
		"docker/abc":                "/docker/abc",
		"/docker/abc/":              "/docker/abc",
		"/sys/fs/cgroup/docker/abc": "/sys/fs/cgroup/docker/abc",
	}
	for in, want := range cases {
		assert.Equal(t, want, normalizeCgroupPath(in), "input %q", in)
	}
}
