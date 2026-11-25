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

package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/cadvisor/lib/scheduler"
)

func waitForCount(t *testing.T, got *atomic.Int64, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("count = %d, want >= %d within %s", got.Load(), want, timeout)
}

func TestSchedulerRunsTaskAtConfiguredInterval(t *testing.T) {
	s := scheduler.NewScheduler(4)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var calls atomic.Int64
	err := s.AddCollector("container-a", "cpu", scheduler.TaskFunc(func(context.Context) error {
		calls.Add(1)
		return nil
	}), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	waitForCount(t, &calls, 5, time.Second)
}

type timeoutFakeCollector struct {
	calls atomic.Int64
}

func (c *timeoutFakeCollector) Collect(ctx context.Context) error {
	c.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestSchedulerTimeoutDoesNotStopLaterRounds(t *testing.T) {
	s := scheduler.NewScheduler(8, 10*time.Millisecond, 25*time.Millisecond)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := &timeoutFakeCollector{}
	if err := s.AddCollector("container-timeout", c); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.calls.Load() >= 4 && s.ErrorCount("container-timeout", "timeoutFakeCollector") >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("calls=%d errors=%d; expected repeated timed-out rounds",
		c.calls.Load(), s.ErrorCount("container-timeout", "timeoutFakeCollector"))
}

type concurrencyFake struct {
	active    atomic.Int32
	maxActive atomic.Int32
	release   chan struct{}
}

func (c *concurrencyFake) Run(ctx context.Context) error {
	n := c.active.Add(1)
	for {
		max := c.maxActive.Load()
		if n <= max || c.maxActive.CompareAndSwap(max, n) {
			break
		}
	}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSchedulerLimitsConcurrency(t *testing.T) {
	const limit = 2
	s := scheduler.NewScheduler(limit)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	fakes := make([]*concurrencyFake, 6)
	for i := range fakes {
		f := &concurrencyFake{release: make(chan struct{})}
		fakes[i] = f
		if err := s.AddCollector("busy", string(rune('a'+i)), f, time.Hour); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && s.ActiveCount() < int64(limit) {
		time.Sleep(time.Millisecond)
	}
	if got := s.ActiveCount(); got != limit {
		t.Fatalf("active collectors = %d, want %d", got, limit)
	}

	time.Sleep(50 * time.Millisecond)
	var max int32
	for _, f := range fakes {
		if got := f.maxActive.Load(); got > max {
			max = got
		}
	}
	if max > limit {
		t.Fatalf("max concurrent collectors = %d, want <= %d", max, limit)
	}

	for _, f := range fakes {
		close(f.release)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSlowCollectorDoesNotBlockFastCollector(t *testing.T) {
	s := scheduler.NewScheduler(2)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	slowEntered := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowOnce sync.Once
	if err := s.AddCollector("slow-container", "slow", scheduler.TaskFunc(func(ctx context.Context) error {
		slowOnce.Do(func() { close(slowEntered) })
		select {
		case <-releaseSlow:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), time.Hour); err != nil {
		t.Fatal(err)
	}

	select {
	case <-slowEntered:
	case <-time.After(time.Second):
		t.Fatal("slow collector did not start")
	}

	var fastCalls atomic.Int64
	if err := s.AddCollector("fast-container", "fast", scheduler.TaskFunc(func(context.Context) error {
		fastCalls.Add(1)
		return nil
	}), 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitForCount(t, &fastCalls, 3, time.Second)
	close(releaseSlow)
}

func TestRemoveContainerStopsFutureCollectionsAndCounters(t *testing.T) {
	s := scheduler.NewScheduler(2)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var calls atomic.Int64
	if err := s.AddCollector("ephemeral", "cpu", scheduler.TaskFunc(func(context.Context) error {
		calls.Add(1)
		return errors.New("boom")
	}), 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitForCount(t, &calls, 3, time.Second)

	if err := s.RemoveContainer("ephemeral"); err != nil {
		t.Fatal(err)
	}
	afterRemove := calls.Load()
	time.Sleep(80 * time.Millisecond)
	if got := calls.Load(); got != afterRemove {
		t.Fatalf("calls after remove = %d, want stable at %d", got, afterRemove)
	}
	if got := s.ErrorCount("ephemeral", "cpu"); got != 0 {
		t.Fatalf("ErrorCount after remove = %d, want 0", got)
	}
}

func TestErrorsAreCountedByContainerAndCollector(t *testing.T) {
	s := scheduler.NewScheduler(2)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.AddCollector("c1", "disk", scheduler.TaskFunc(func(context.Context) error {
		return errors.New("disk unavailable")
	}), 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := s.AddCollector("c1", "net", scheduler.TaskFunc(func(context.Context) error {
		return nil
	}), 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.ErrorCount("c1", "disk") >= 3 {
			if got := s.ErrorCount("c1", "net"); got != 0 {
				t.Fatalf("net errors = %d, want 0", got)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("disk errors = %d, want >= 3", s.ErrorCount("c1", "disk"))
}

func TestPanicIsCountedAndDoesNotEscape(t *testing.T) {
	s := scheduler.NewScheduler(2)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var calls atomic.Int64
	if err := s.AddCollector("panic-container", "panic-collector", scheduler.TaskFunc(func(context.Context) error {
		calls.Add(1)
		panic("boom")
	}), 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.ErrorCount("panic-container", "panic-collector") >= 2 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("panic errors = %d calls=%d", s.ErrorCount("panic-container", "panic-collector"), calls.Load())
}

func TestConcurrentStartAndCloseIsSafe(t *testing.T) {
	for i := 0; i < 50; i++ {
		s := scheduler.NewScheduler(2)
		go func() { _ = s.Start() }()
		_ = s.Close()
		_ = s.Close()
	}
}

func TestCloseWaitsForInFlightWorkAndIsIdempotent(t *testing.T) {
	s := scheduler.NewScheduler(2)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	if err := s.AddCollector("closing", "blocking", scheduler.TaskFunc(func(context.Context) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}), time.Hour); err != nil {
		t.Fatal(err)
	}
	<-entered

	closed := make(chan error, 2)
	go func() { closed <- s.Close() }()
	go func() { closed <- s.Close() }()

	select {
	case err := <-closed:
		t.Fatalf("Close returned before in-flight collection finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not converge after releasing collector")
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("third Close returned error: %v", err)
	}
}
