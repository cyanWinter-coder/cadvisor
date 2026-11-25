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

package manager

import (
	"sync"
	"testing"
	"time"

	"github.com/google/cadvisor/lib/cache/memory"
	info "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
)

type hardeningBlockingStatsHandler struct {
	*mockHandler
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *hardeningBlockingStatsHandler) GetStats() (*info.ContainerStats, error) {
	h.once.Do(func() { close(h.entered) })
	<-h.release
	return &info.ContainerStats{Timestamp: time.Now()}, nil
}

func TestContainerStopWaitsForInFlightCollectionAndRemovesCache(t *testing.T) {
	cache := memory.New(time.Minute, nil)
	handler := &hardeningBlockingStatsHandler{
		mockHandler: &mockHandler{
			ref:  info.ContainerReference{Name: "/hardening-wait"},
			spec: info.ContainerSpec{HasCpu: true},
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	cd, err := newContainerData("/hardening-wait", cache, handler, time.Minute, false, clock.RealClock{})
	require.NoError(t, err)
	require.NoError(t, cd.Start())

	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("housekeeping did not enter GetStats")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- cd.Stop() }()

	select {
	case err := <-stopped:
		t.Fatalf("Stop returned before in-flight collection converged: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(handler.release)
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after GetStats was released")
	}

	_, err = cache.RecentStats("/hardening-wait", time.Time{}, time.Time{}, 1)
	require.ErrorIs(t, err, memory.ErrDataNotFound)

	done := make(chan error, 1)
	go func() { done <- cd.Stop() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second Stop was not idempotent")
	}
}
