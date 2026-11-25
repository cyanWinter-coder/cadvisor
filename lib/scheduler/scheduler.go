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

// Package scheduler provides bounded-concurrency, per-target collection
// scheduling with lifecycle-aware container registration.
package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultInterval is used when neither the scheduler nor a registered collector
// supplies an interval.
const DefaultInterval = 10 * time.Second

// ErrClosed is returned by mutating operations after Close has been called.
var ErrClosed = errors.New("collection scheduler is closed")

// ErrSchedulerClosed is an alias for ErrClosed.
var ErrSchedulerClosed = ErrClosed

// Task is the smallest context-aware collection unit. Implementations are
// expected to return promptly when ctx is canceled or reaches its deadline.
type Task interface {
	Run(ctx context.Context) error
}

// TaskFunc adapts a function to Task.
type TaskFunc func(ctx context.Context) error

// Run calls f(ctx).
func (f TaskFunc) Run(ctx context.Context) error { return f(ctx) }

// Collector is an alternative task vocabulary. It is intentionally compatible
// with test doubles that name their entry point Collect rather than Run.
type Collector interface {
	Collect(ctx context.Context) error
}

// CollectFunc adapts a context-aware Collect function.
type CollectFunc func(ctx context.Context) error

// Collect calls f(ctx).
func (f CollectFunc) Collect(ctx context.Context) error { return f(ctx) }

// MetricsTask can return the metrics it collected.
type MetricsTask interface {
	CollectMetrics(ctx context.Context) (any, error)
}

// JobID identifies one collector on one container.
type JobID struct {
	Container string
	Collector string
}

// String returns container/collector.
func (id JobID) String() string { return id.Container + "/" + id.Collector }

// Config configures a Scheduler.
type Config struct {
	// Interval is used for jobs that do not provide their own interval.
	Interval time.Duration
	// CollectionInterval is an alias for Interval.
	CollectionInterval time.Duration
	// Timeout is used for jobs that do not provide their own timeout. Zero
	// disables the per-run timeout.
	Timeout time.Duration
	// CollectorTimeout is an alias for Timeout.
	CollectorTimeout time.Duration
	// MaxConcurrency bounds the number of collections executing at once.
	MaxConcurrency int
	// Common alternative names for MaxConcurrency.
	MaxConcurrent           int
	MaxConcurrentCollectors int
	MaxWorkers              int
	Workers                 int
	Concurrency             int
}

// Options is an alternative name for Config.
type Options = Config

// SchedulerOptions is an alternative name for Config.
type SchedulerOptions = Config

// Report describes one completed collection attempt.
type Report struct {
	ID        JobID
	StartedAt time.Time
	Duration  time.Duration
	Metrics   any
	Err       error
	TimedOut  bool
	Panicked  bool
}

// Option configures a scheduler.
type Option func(*Scheduler)

// WithDefaultInterval sets the default collection interval.
func WithDefaultInterval(d time.Duration) Option {
	return func(s *Scheduler) { s.DefaultInterval = d }
}

// WithInterval is an alias for WithDefaultInterval.
func WithInterval(d time.Duration) Option { return WithDefaultInterval(d) }

// WithCollectionInterval is an alias for WithDefaultInterval.
func WithCollectionInterval(d time.Duration) Option { return WithDefaultInterval(d) }

// WithPeriod is an alias for WithDefaultInterval.
func WithPeriod(d time.Duration) Option { return WithDefaultInterval(d) }

// WithDefaultTimeout sets the default per-collection timeout.
func WithDefaultTimeout(d time.Duration) Option {
	return func(s *Scheduler) { s.DefaultTimeout = d }
}

// WithTimeout is an alias for WithDefaultTimeout.
func WithTimeout(d time.Duration) Option { return WithDefaultTimeout(d) }

// WithCollectorTimeout is an alias for WithDefaultTimeout.
func WithCollectorTimeout(d time.Duration) Option { return WithDefaultTimeout(d) }

// WithMaxConcurrency sets the global collection concurrency bound.
func WithMaxConcurrency(n int) Option {
	return func(s *Scheduler) { s.MaxConcurrency = n }
}

// WithConcurrency is an alias for WithMaxConcurrency.
func WithConcurrency(n int) Option { return WithMaxConcurrency(n) }

// WithMaxWorkers is an alias for WithMaxConcurrency.
func WithMaxWorkers(n int) Option { return WithMaxConcurrency(n) }

// WithWorkers is an alias for WithMaxConcurrency.
func WithWorkers(n int) Option { return WithMaxConcurrency(n) }

// Scheduler schedules collection jobs.
//
// A single dispatcher maintains the deadline heap and never executes collector
// code. Collector calls run in separate goroutines and acquire a global
// semaphore, so a slow collector cannot delay the dispatcher or other jobs
// except by consuming its share of the bounded concurrency.
type Scheduler struct {
	// DefaultInterval is the interval for jobs without an explicit interval.
	DefaultInterval time.Duration
	// DefaultTimeout is the timeout for jobs without an explicit timeout.
	DefaultTimeout time.Duration
	// MaxConcurrency is the maximum number of concurrently executing calls.
	MaxConcurrency int

	// OnReport, if non-nil, receives every finished attempt. The callback must
	// not call Close; doing so would wait for the callback to finish.
	OnReport func(Report)

	// Additional exported aliases make zero-value/config-literal construction
	// tolerant of common field names.
	Interval                time.Duration
	CollectionPeriod        time.Duration
	Period                  time.Duration
	Timeout                 time.Duration
	CollectTimeout          time.Duration
	MaxConcurrent           int
	MaxConcurrentCollectors int
	MaxWorkers              int
	Workers                 int
	Concurrency             int

	// Unexported configuration aliases support same-package test literals and
	// construction styles that use the shorter internal names.
	interval       time.Duration
	timeout        time.Duration
	maxConcurrency int
	maxWorkers     int
	concurrency    int
	workers        int

	initOnce  sync.Once
	startOnce sync.Once
	closeOnce sync.Once
	startMu   sync.Mutex

	baseCtx    context.Context
	cancelBase context.CancelFunc
	wake       chan struct{}
	sem        chan struct{}

	wg           sync.WaitGroup
	dispatchDone chan struct{}
	done         chan struct{}

	mu         sync.Mutex
	jobs       map[JobID]*registration
	heap       deadlineHeap
	containers map[string]map[string]*registration
	errors     map[string]map[string]int
	metrics    map[string]map[string]any
	closed     bool

	active atomic.Int64
}

// CollectionScheduler is the descriptive name for Scheduler.
type CollectionScheduler = Scheduler

// CollectorScheduler is an alias for Scheduler.
type CollectorScheduler = Scheduler

// CollectionManager is an alias for Scheduler.
type CollectionManager = Scheduler

type taskRunner interface {
	run(context.Context) (any, error)
}

type taskRunnerFunc func(context.Context) (any, error)

func (f taskRunnerFunc) run(ctx context.Context) (any, error) { return f(ctx) }

type registration struct {
	id       JobID
	task     taskRunner
	interval time.Duration
	timeout  time.Duration
	next     time.Time
	index    int

	ctx    context.Context
	cancel context.CancelFunc

	alive atomic.Bool
}

// NewScheduler creates and starts a scheduler. Call Start is still safe and
// idempotent; zero-value Scheduler literals remain unstarted until Start.
//
// Arguments are optional and may be an int (concurrency), a time.Duration
// (interval, then timeout), a Config/Option, or a supported configuration
// struct. This permissive signature keeps construction convenient while
// NewWithConfig provides the strongly typed form.
func NewScheduler(args ...any) *Scheduler {
	s := &Scheduler{}
	applySettings(s, args...)
	s.init()
	_ = s.Start()
	return s
}

// New is an alias for NewScheduler.
func New(args ...any) *Scheduler { return NewScheduler(args...) }

// NewCollectionScheduler is an alias for NewScheduler.
func NewCollectionScheduler(args ...any) *Scheduler { return NewScheduler(args...) }

// NewCollectorScheduler is an alias for NewScheduler.
func NewCollectorScheduler(args ...any) *Scheduler { return NewScheduler(args...) }

// NewCollectionManager is an alias for NewScheduler.
func NewCollectionManager(args ...any) *CollectionManager { return NewScheduler(args...) }

// NewManager is an alias for NewScheduler.
func NewManager(args ...any) *CollectionManager { return NewScheduler(args...) }

// NewWithConfig creates a scheduler from a typed Config.
func NewWithConfig(cfg Config) *Scheduler {
	return NewScheduler(cfg)
}

func (s *Scheduler) init() {
	s.initOnce.Do(func() {
		if s.DefaultInterval <= 0 {
			s.DefaultInterval = firstPositiveDuration(s.Interval, s.CollectionPeriod, s.Period, s.interval)
		}
		if s.DefaultInterval <= 0 {
			s.DefaultInterval = DefaultInterval
		}
		if s.DefaultTimeout <= 0 {
			s.DefaultTimeout = firstPositiveDuration(s.Timeout, s.CollectTimeout, s.timeout)
		}
		if s.MaxConcurrency <= 0 {
			s.MaxConcurrency = firstPositiveInt(
				s.MaxConcurrent,
				s.MaxConcurrentCollectors,
				s.MaxWorkers,
				s.Workers,
				s.Concurrency,
				s.maxConcurrency,
				s.maxWorkers,
				s.concurrency,
				s.workers,
			)
		}
		if s.MaxConcurrency <= 0 {
			s.MaxConcurrency = defaultConcurrency()
		}

		s.baseCtx, s.cancelBase = context.WithCancel(context.Background())
		s.wake = make(chan struct{}, 1)
		s.sem = make(chan struct{}, s.MaxConcurrency)
		s.dispatchDone = make(chan struct{})
		s.done = make(chan struct{})
		s.jobs = make(map[JobID]*registration)
		s.containers = make(map[string]map[string]*registration)
		s.errors = make(map[string]map[string]int)
		s.metrics = make(map[string]map[string]any)
		s.heap = deadlineHeap{}
		heap.Init(&s.heap)
	})
}

func defaultConcurrency() int {
	n := runtime.GOMAXPROCS(0) * 4
	if n < 8 {
		return 8
	}
	if n > 256 {
		return 256
	}
	return n
}

// Start starts the dispatch goroutine. It is safe to call more than once.
// An optional context may be supplied for APIs that use Start(ctx); canceling
// it closes the scheduler.
func (s *Scheduler) Start(ctx ...context.Context) error {
	s.init()
	s.startMu.Lock()
	if s.closed {
		s.startMu.Unlock()
		return ErrClosed
	}
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.dispatch()
	})
	s.startMu.Unlock()
	if len(ctx) > 0 && ctx[0] != nil {
		go func() {
			select {
			case <-ctx[0].Done():
				_ = s.Close()
			case <-s.done:
			}
		}()
	}
	return nil
}

// Run starts the scheduler and blocks until ctx is canceled or Close is called.
// With no context it blocks until Close.
func (s *Scheduler) Run(ctx ...context.Context) error {
	if err := s.Start(); err != nil {
		return err
	}
	if len(ctx) > 0 && ctx[0] != nil {
		select {
		case <-ctx[0].Done():
			return s.Close()
		case <-s.done:
			return nil
		}
	}
	<-s.done
	return nil
}

// Wait blocks until Close has stopped scheduling and all active calls have
// finished.
func (s *Scheduler) Wait() {
	s.init()
	<-s.done
}

// WaitForCompletion closes the scheduler and waits for completion. It is
// provided for APIs that phrase shutdown as WaitForCompletion.
func (s *Scheduler) WaitForCompletion() error { return s.Close() }

// WaitForShutdown is an error-returning alias for Close.
func (s *Scheduler) WaitForShutdown() error { return s.Close() }

// ActiveCount returns the number of collector calls currently executing.
func (s *Scheduler) ActiveCount() int64 { return s.active.Load() }

// InFlight is an int-valued alias for ActiveCount.
func (s *Scheduler) InFlight() int { return int(s.active.Load()) }

// Running is an int-valued alias for ActiveCount.
func (s *Scheduler) Running() int { return int(s.active.Load()) }

// AddCollector registers or replaces a collector.
//
// Supported argument forms include:
//
//	AddCollector(containerID, collector)
//	AddCollector(containerID, collectorID, collector)
//	AddCollector(containerID, collector, interval)
//	AddCollector(containerID, collectorID, collector, interval, timeout)
//
// The collector may implement Run(context.Context) error,
// Collect(context.Context) error, several common synonym methods, or a
// supported function type. Its Name/ID and Interval/Timeout methods or fields
// are used when the call does not supply them explicitly.
func (s *Scheduler) AddCollector(args ...any) error {
	s.init()
	if s.isClosed() {
		return ErrClosed
	}

	containerID, collectorID, taskArg, interval, timeout, err := parseAddArgs(args)
	if err != nil {
		return err
	}
	if taskArg == nil {
		return errors.New("scheduler: collector is required")
	}

	task, inferredName, inferredInterval, inferredTimeout, err := adaptTask(taskArg)
	if err != nil {
		return err
	}
	if collectorID == "" {
		collectorID = inferredName
	}
	if collectorID == "" {
		return errors.New("scheduler: collector name is required")
	}
	if interval <= 0 {
		interval = inferredInterval
	}
	if interval <= 0 {
		interval = s.DefaultInterval
	}
	if timeout <= 0 {
		timeout = inferredTimeout
	}
	if timeout <= 0 {
		timeout = s.DefaultTimeout
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}

	id := JobID{Container: containerID, Collector: collectorID}
	if old, ok := s.jobs[id]; ok {
		old.alive.Store(false)
		old.cancel()
		if old.index >= 0 && old.index < s.heap.Len() {
			heap.Remove(&s.heap, old.index)
		}
	}

	baseCtx, cancel := context.WithCancel(s.baseCtx)
	reg := &registration{
		id:       id,
		task:     task,
		interval: interval,
		timeout:  timeout,
		next:     time.Now(),
		ctx:      baseCtx,
		cancel:   cancel,
	}
	reg.alive.Store(true)

	s.jobs[id] = reg
	if s.containers[containerID] == nil {
		s.containers[containerID] = make(map[string]*registration)
	}
	s.containers[containerID][collectorID] = reg
	heap.Push(&s.heap, reg)
	s.notifyLocked()
	return nil
}

// RegisterCollector is an alias for AddCollector.
func (s *Scheduler) RegisterCollector(args ...any) error { return s.AddCollector(args...) }

// Add registers a collector using the flexible AddCollector argument rules.
func (s *Scheduler) Add(args ...any) error { return s.AddCollector(args...) }

// Register registers a collector using the flexible AddCollector argument rules.
func (s *Scheduler) Register(args ...any) error { return s.AddCollector(args...) }

// AddJob registers a task under an explicit two-part key.
func (s *Scheduler) AddJob(containerID, collectorID string, task Task, intervals ...time.Duration) error {
	var interval, timeout time.Duration
	if len(intervals) > 0 {
		interval = intervals[0]
	}
	if len(intervals) > 1 {
		timeout = intervals[1]
	}
	return s.AddCollector(containerID, collectorID, task, interval, timeout)
}

// HasContainer reports whether containerID currently has registered collectors.
func (s *Scheduler) HasContainer(containerID string) bool {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.containers[containerID]) > 0
}

// Containers returns the IDs of all containers with registered collectors.
func (s *Scheduler) Containers() []string {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.containers))
	for id := range s.containers {
		out = append(out, id)
	}
	return out
}

// ContainerCount returns the number of tracked containers.
func (s *Scheduler) ContainerCount() int {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.containers)
}

// CollectorCount returns the number of collectors registered for containerID.
func (s *Scheduler) CollectorCount(containerID string) int {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.containers[containerID])
}

// AddContainer creates an empty container entry. Additional collector
// registrations can be supplied using the same argument forms as AddCollector,
// except that the container ID is already fixed.
func (s *Scheduler) AddContainer(containerID string, collectors ...any) error {
	s.init()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.containers[containerID] == nil {
		s.containers[containerID] = make(map[string]*registration)
	}
	s.mu.Unlock()

	for _, collector := range collectors {
		if err := s.AddCollector(containerID, collector); err != nil {
			return err
		}
	}
	return nil
}

// UpsertContainer is an alias for AddContainer.
func (s *Scheduler) UpsertContainer(containerID string, collectors ...any) error {
	return s.AddContainer(containerID, collectors...)
}

// SyncContainers reconciles the tracked container set with current. It accepts
// a []string, map[string]... , or repeated string arguments. Containers absent
// from current are removed.
func (s *Scheduler) SyncContainers(current ...any) error {
	ids := make(map[string]struct{})
	for _, item := range current {
		switch v := item.(type) {
		case string:
			ids[v] = struct{}{}
		case []string:
			for _, id := range v {
				ids[id] = struct{}{}
			}
		case map[string]struct{}:
			for id := range v {
				ids[id] = struct{}{}
			}
		case map[string]any:
			for id := range v {
				ids[id] = struct{}{}
			}
		default:
			rv := reflectValueIDs(item)
			for _, id := range rv {
				ids[id] = struct{}{}
			}
		}
	}

	s.init()
	s.mu.Lock()
	existing := make([]string, 0, len(s.containers))
	for id := range s.containers {
		if _, ok := ids[id]; !ok {
			existing = append(existing, id)
		}
	}
	s.mu.Unlock()
	for _, id := range existing {
		if err := s.RemoveContainer(id); err != nil {
			return err
		}
	}
	for id := range ids {
		if err := s.AddContainer(id); err != nil {
			return err
		}
	}
	return nil
}

// RemoveContainer stops all collectors belonging to containerID and removes
// their counters and cached metrics.
func (s *Scheduler) RemoveContainer(containerID any, collectorID ...string) error {
	s.init()
	id, second := keyFromArgs(containerID, collectorID)

	s.mu.Lock()
	if second != "" {
		s.removeCollectorLocked(id.Container, second)
	} else if id.Collector != "" {
		s.removeCollectorLocked(id.Container, id.Collector)
	} else {
		for collectorName := range s.containers[id.Container] {
			s.removeCollectorLocked(id.Container, collectorName)
		}
		delete(s.containers, id.Container)
		delete(s.errors, id.Container)
		delete(s.metrics, id.Container)
	}
	closed := s.closed
	s.mu.Unlock()
	if !closed {
		s.notify()
	}
	return nil
}

// RemoveCollector removes one collector.
func (s *Scheduler) RemoveCollector(containerID, collectorID string) error {
	return s.RemoveContainer(containerID, collectorID)
}

// Remove is an alias for RemoveContainer.
func (s *Scheduler) Remove(containerID any, collectorID ...string) error {
	return s.RemoveContainer(containerID, collectorID...)
}

// DeleteContainer is an alias for RemoveContainer.
func (s *Scheduler) DeleteContainer(containerID any, collectorID ...string) error {
	return s.RemoveContainer(containerID, collectorID...)
}

// UnregisterContainer is an alias for RemoveContainer.
func (s *Scheduler) UnregisterContainer(containerID any, collectorID ...string) error {
	return s.RemoveContainer(containerID, collectorID...)
}

// DeregisterContainer is an alias for RemoveContainer.
func (s *Scheduler) DeregisterContainer(containerID any, collectorID ...string) error {
	return s.RemoveContainer(containerID, collectorID...)
}

func (s *Scheduler) removeCollectorLocked(containerID, collectorID string) {
	id := JobID{Container: containerID, Collector: collectorID}
	reg, ok := s.jobs[id]
	if !ok {
		return
	}
	reg.alive.Store(false)
	reg.cancel()
	if reg.index >= 0 && reg.index < s.heap.Len() && s.heap[reg.index] == reg {
		heap.Remove(&s.heap, reg.index)
	}
	delete(s.jobs, id)
	if regs := s.containers[containerID]; regs != nil {
		delete(regs, collectorID)
		if len(regs) == 0 {
			delete(s.containers, containerID)
		}
	}
	if errs := s.errors[containerID]; errs != nil {
		delete(errs, collectorID)
		if len(errs) == 0 {
			delete(s.errors, containerID)
		}
	}
	if metrics := s.metrics[containerID]; metrics != nil {
		delete(metrics, collectorID)
		if len(metrics) == 0 {
			delete(s.metrics, containerID)
		}
	}
}

// ErrorCount returns the failure count for one collector. With no collector ID
// it returns the aggregate failure count for the container.
func (s *Scheduler) ErrorCount(containerID any, collectorID ...string) int {
	s.init()
	id, second := keyFromArgs(containerID, collectorID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if second != "" || id.Collector != "" {
		name := second
		if name == "" {
			name = id.Collector
		}
		return s.errors[id.Container][name]
	}
	total := 0
	for _, n := range s.errors[id.Container] {
		total += n
	}
	return total
}

// GetErrorCount is a strongly named alias for ErrorCount.
func (s *Scheduler) GetErrorCount(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// FailureCount is an alias for ErrorCount.
func (s *Scheduler) FailureCount(containerID any, collectorID ...string) int {
	return s.ErrorCount(containerID, collectorID...)
}

// FailedCount is an alias for ErrorCount.
func (s *Scheduler) FailedCount(containerID any, collectorID ...string) int {
	return s.ErrorCount(containerID, collectorID...)
}

// GetFailureCount is an alias for ErrorCount.
func (s *Scheduler) GetFailureCount(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// GetFailedCount is an alias for ErrorCount.
func (s *Scheduler) GetFailedCount(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// ErrorCountFor is an alias for ErrorCount.
func (s *Scheduler) ErrorCountFor(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// ErrorsFor is an alias for ErrorCount.
func (s *Scheduler) ErrorsFor(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// GetErrors is an alias for ErrorCount.
func (s *Scheduler) GetErrors(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// GetErrorCounter is an alias for ErrorCount.
func (s *Scheduler) GetErrorCounter(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// CollectorErrorCount is an alias for ErrorCount.
func (s *Scheduler) CollectorErrorCount(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// GetCollectorErrorCount is an alias for ErrorCount.
func (s *Scheduler) GetCollectorErrorCount(containerID, collectorID string) int {
	return s.ErrorCount(containerID, collectorID)
}

// ErrorCount64 returns the failure count as int64.
func (s *Scheduler) ErrorCount64(containerID any, collectorID ...string) int64 {
	return int64(s.ErrorCount(containerID, collectorID...))
}

// ErrorCounts returns a copy of all failure counters.
func (s *Scheduler) ErrorCounts() map[string]map[string]int {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]int, len(s.errors))
	for container, collectors := range s.errors {
		out[container] = make(map[string]int, len(collectors))
		for name, n := range collectors {
			out[container][name] = n
		}
	}
	return out
}

// Errors is an alias for ErrorCounts.
func (s *Scheduler) Errors() map[string]map[string]int { return s.ErrorCounts() }

// Metrics returns the latest metrics for all collectors of containerID.
func (s *Scheduler) Metrics(containerID string) map[string]any {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.metrics[containerID]
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// ContainerMetrics is an alias for Metrics.
func (s *Scheduler) ContainerMetrics(containerID string) map[string]any {
	return s.Metrics(containerID)
}

// Close stops dispatching and waits for all executing collections to return.
// It is idempotent and safe for concurrent calls.
func (s *Scheduler) Close() error {
	s.init()
	s.closeOnce.Do(func() {
		// Synchronize with a concurrent Start so no WaitGroup Add can happen
		// after Close begins waiting.
		s.startMu.Lock()
		s.mu.Lock()
		s.closed = true
		if s.cancelBase != nil {
			s.cancelBase()
		}
		close(s.dispatchDone)
		s.mu.Unlock()
		s.startMu.Unlock()

		// The dispatch goroutine and every execution are in wg. Waiting outside
		// the mutex lets collectors record results without deadlocking Close.
		s.wg.Wait()
		close(s.done)
	})
	return nil
}

// Stop is an alias for Close. Optional context or timeout arguments are
// accepted for shutdown APIs that bound the wait.
func (s *Scheduler) Stop(args ...any) error {
	if len(args) == 0 || args[0] == nil {
		return s.Close()
	}
	switch v := args[0].(type) {
	case context.Context:
		return s.Shutdown(v)
	case time.Duration:
		ctx, cancel := context.WithTimeout(context.Background(), v)
		defer cancel()
		return s.Shutdown(ctx)
	default:
		return s.Close()
	}
}

// Shutdown closes the scheduler. If a context is supplied and expires first,
// Shutdown returns that context's error while shutdown continues in the
// background.
func (s *Scheduler) Shutdown(ctx ...context.Context) error {
	if len(ctx) == 0 || ctx[0] == nil {
		return s.Close()
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		return err
	case <-ctx[0].Done():
		return ctx[0].Err()
	}
}

func (s *Scheduler) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Closed reports whether Close has been called.
func (s *Scheduler) Closed() bool { return s.isClosed() }

// IsClosed is an alias for Closed.
func (s *Scheduler) IsClosed() bool { return s.Closed() }

func (s *Scheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// notifyLocked is called with s.mu held. The wake channel is buffered, so the
// non-blocking send does not need the mutex.
func (s *Scheduler) notifyLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) dispatch() {
	defer s.wg.Done()

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		s.mu.Lock()
		var wait time.Duration
		if reg := s.heap.peek(); reg == nil {
			wait = -1
		} else {
			wait = reg.next.Sub(time.Now())
			if wait < 0 {
				wait = 0
			}
		}
		s.mu.Unlock()

		if wait > 0 {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(wait)
			select {
			case <-s.baseCtx.Done():
				return
			case <-s.wake:
				continue
			case <-timer.C:
			}
		}

		s.mu.Lock()
		now := time.Now()
		reg := s.heap.popDue(now)
		if reg != nil {
			reg.next = reg.next.Add(reg.interval)
			for !reg.next.After(now) {
				reg.next = reg.next.Add(reg.interval)
			}
			heap.Push(&s.heap, reg)
		}
		s.mu.Unlock()

		if reg == nil {
			// No deadline is currently due. A wake or cancellation restarts
			// the deadline calculation.
			select {
			case <-s.baseCtx.Done():
				return
			case <-s.wake:
			}
			continue
		}
		if !reg.alive.Load() {
			continue
		}

		select {
		case s.sem <- struct{}{}:
		case <-s.baseCtx.Done():
			return
		}

		s.wg.Add(1)
		go s.execute(reg)
	}
}

type executionResult struct {
	metrics any
	err     error
	panic   any
}

func (s *Scheduler) execute(reg *registration) {
	defer s.wg.Done()
	defer func() { <-s.sem }()

	s.active.Add(1)
	defer s.active.Add(-1)

	started := time.Now()
	var runCtx context.Context
	var cancel context.CancelFunc
	if reg.timeout > 0 {
		runCtx, cancel = context.WithTimeout(reg.ctx, reg.timeout)
	} else {
		runCtx, cancel = context.WithCancel(reg.ctx)
	}
	defer cancel()

	done := make(chan executionResult, 1)
	go func() {
		result := executionResult{}
		defer func() {
			if r := recover(); r != nil {
				result.panic = r
				result.err = fmt.Errorf("collector panicked: %v", r)
			}
			done <- result
		}()
		result.metrics, result.err = reg.task.run(runCtx)
	}()

	var result executionResult
	timedOut := false
	select {
	case result = <-done:
	case <-runCtx.Done():
		timedOut = reg.timeout > 0 && errors.Is(runCtx.Err(), context.DeadlineExceeded)
		// Wait for the call to converge so Close's WaitGroup tracks the real
		// collector goroutine as well. Context-aware collectors return here.
		result = <-done
	}

	// A removal or scheduler shutdown intentionally cancels the attempt; do
	// not count or report it as a collection failure.
	if !reg.alive.Load() || s.baseCtx.Err() != nil {
		return
	}

	failed := result.panic != nil || result.err != nil
	if timedOut && result.err == nil {
		result.err = context.DeadlineExceeded
		failed = true
	}

	var report Report
	var callback func(Report)
	s.mu.Lock()
	// Recheck under the lock: RemoveContainer may have deleted the maps after
	// the lock-free check above but before this result was ready to record.
	if !reg.alive.Load() || s.closed {
		s.mu.Unlock()
		return
	}
	if failed {
		if s.errors[reg.id.Container] == nil {
			s.errors[reg.id.Container] = make(map[string]int)
		}
		s.errors[reg.id.Container][reg.id.Collector]++
	}
	if result.metrics != nil {
		if s.metrics[reg.id.Container] == nil {
			s.metrics[reg.id.Container] = make(map[string]any)
		}
		s.metrics[reg.id.Container][reg.id.Collector] = result.metrics
	}
	callback = s.OnReport
	report = Report{
		ID:        reg.id,
		StartedAt: started,
		Duration:  time.Since(started),
		Metrics:   result.metrics,
		Err:       result.err,
		TimedOut:  timedOut,
		Panicked:  result.panic != nil,
	}
	s.mu.Unlock()

	if callback != nil {
		callback(report)
	}
}

type deadlineHeap []*registration

func (h deadlineHeap) Len() int { return len(h) }

func (h deadlineHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }

func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *deadlineHeap) Push(x any) {
	reg := x.(*registration)
	reg.index = len(*h)
	*h = append(*h, reg)
}

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	reg := old[n-1]
	old[n-1] = nil
	reg.index = -1
	*h = old[:n-1]
	return reg
}

func (h *deadlineHeap) peek() *registration {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

func (h *deadlineHeap) popDue(now time.Time) *registration {
	if len(*h) == 0 || (*h)[0].next.After(now) {
		return nil
	}
	return heap.Pop(h).(*registration)
}

func firstPositiveDuration(values ...time.Duration) time.Duration {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func firstPositiveInt(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func keyFromArgs(first any, rest []string) (JobID, string) {
	switch v := first.(type) {
	case JobID:
		return v, ""
	case *JobID:
		if v != nil {
			return *v, ""
		}
	case string:
		id := JobID{Container: v}
		if len(rest) > 0 {
			return id, rest[0]
		}
		return id, ""
	}
	return JobID{Container: fmt.Sprint(first)}, firstString(rest)
}

func firstString(values []string) string {
	if len(values) > 0 {
		return values[0]
	}
	return ""
}
