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

package buffered

import (
	"errors"
	"sync"
	"time"

	info "github.com/google/cadvisor/lib/model"

	"k8s.io/klog/v2"
)

// BatchWriter writes one batch of samples to a backend. The batch preserves
// the enqueue order. Implementations must be safe for concurrent use when
// the exporter concurrency is greater than one.
type BatchWriter interface {
	WriteBatch(batch []Sample) error
	Close() error
}

// Sample is one export item: the arguments of StorageDriver.AddStats.
type Sample struct {
	ContainerInfo *info.ContainerInfo
	Stats         *info.ContainerStats
}

// Config configures an exporter. Zero values are replaced with sane defaults
// in NewExporter, so callers only need to set the fields they care about.
type Config struct {
	// MaxBatchSize is the maximum number of samples sent in one backend
	// write. A write is also triggered once Linger elapses with queued data.
	MaxBatchSize int
	// Concurrency is the number of worker goroutines performing writes.
	Concurrency int
	// Linger is how long a worker waits for a partial batch to fill up
	// before flushing whatever is queued. It replaces the old per-driver
	// buffer duration and keeps low-traffic deployments writing promptly.
	Linger time.Duration
	// MaxQueueSize bounds the number of samples held in memory. Samples
	// beyond this limit cause the oldest queued samples to be dropped.
	MaxQueueSize int
	// MaxRetries is the number of retries for a failed batch (on top of the
	// initial attempt). After the last retry the batch is discarded and
	// counted in Stats.RetryExhausted.
	MaxRetries int
	// InitialBackoff is the delay before the first retry. It doubles on
	// every subsequent retry, up to MaxBackoff.
	InitialBackoff time.Duration
	// MaxBackoff caps the per-retry backoff.
	MaxBackoff time.Duration
	// MaxBatchesPerSecond paces aggregate backend writes across all
	// workers. A value <= 0 disables pacing.
	MaxBatchesPerSecond float64
	// MaxBurst is the token-bucket burst size used with the rate limit.
	MaxBurst int
	// CloseTimeout bounds how long Close waits for the queue to drain.
	CloseTimeout time.Duration
}

// Stats is a point-in-time snapshot of exporter counters.
type Stats struct {
	// Enqueued is the total number of samples accepted into the pipeline.
	Enqueued uint64
	// QueueDropped counts samples discarded because the bounded queue was
	// full (oldest samples are dropped first).
	QueueDropped uint64
	// RetryExhausted counts samples in batches discarded after all retries
	// failed.
	RetryExhausted uint64
	// Written is the total number of samples successfully written.
	Written uint64
	// Retries is the total number of batch write retries performed.
	Retries uint64
	// QueueDepth is the number of samples waiting in the queue.
	QueueDepth int
	// InFlight is the number of samples currently being written.
	InFlight int
}

// Exporter is an asynchronous, memory-bounded and rate-limited export
// pipeline wrapping a BatchWriter. It implements the storage.StorageDriver
// interface; AddStats never blocks on the backend and only returns an error
// when the exporter has been closed.
type Exporter struct {
	writer BatchWriter
	cfg    Config

	queue *queue
	lim   *rateLimiter

	stopOnce  sync.Once
	closeOnce sync.Once
	stop      chan struct{}
	wg        sync.WaitGroup
	closeErr  error

	mu        sync.Mutex
	closed    bool
	enqueued  uint64
	exhausted uint64
	written   uint64
	retries   uint64
	inFlight  int
	// lastDropWarn throttles queue-full warnings while a backend outage is
	// dropping samples continuously.
	lastDropWarn time.Time
}

// ErrClosed is returned by AddStats after Close has been called.
var ErrClosed = errors.New("storage exporter is closed")

// NewExporter builds an exporter with normalized configuration and starts
// its worker pool.
func NewExporter(writer BatchWriter, cfg Config) *Exporter {
	cfg = cfg.withDefaults()
	e := &Exporter{
		writer: writer,
		cfg:    cfg,
		queue:  newQueue(cfg.MaxQueueSize),
		lim:    newRateLimiter(cfg.MaxBatchesPerSecond, cfg.MaxBurst, time.Now()),
		stop:   make(chan struct{}),
	}
	for i := 0; i < cfg.Concurrency; i++ {
		e.wg.Add(1)
		go e.worker()
	}
	e.wg.Add(1)
	go e.statsLoop()
	return e
}

// statsLogInterval is how often the exporter logs its counters. Routine
// summaries make drops and exhausted retries observable without a log line
// per sample while a backend is down for a long time.
const statsLogInterval = 30 * time.Second

// dropWarnInterval throttles the queue-full warning: when a backend outage
// fills the queue, thousands of samples may be dropped in quick succession.
const dropWarnInterval = 10 * time.Second

func (e *Exporter) statsLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(statsLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s := e.Stats()
			klog.V(1).Infof("storage export stats: enqueued=%d written=%d queued=%d in_flight=%d queue_dropped=%d retries=%d retry_exhausted=%d",
				s.Enqueued, s.Written, s.QueueDepth, s.InFlight, s.QueueDropped, s.Retries, s.RetryExhausted)
		case <-e.stop:
			return
		}
	}
}

// AddStats enqueues one sample for asynchronous export. When the in-memory
// queue is full the oldest queued sample is dropped and counted. It never
// blocks on the backend.
func (e *Exporter) AddStats(cInfo *info.ContainerInfo, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.enqueued++
	e.mu.Unlock()

	dropped := e.queue.push(sample{cInfo: cInfo, stats: stats})
	if dropped > 0 {
		e.warnDrop()
	}
	e.queue.signal()
	return nil
}

// warnDrop emits a queue-full warning at most once per dropWarnInterval.
func (e *Exporter) warnDrop() {
	now := time.Now()
	e.mu.Lock()
	shouldLog := e.lastDropWarn.IsZero() || now.Sub(e.lastDropWarn) >= dropWarnInterval
	if shouldLog {
		e.lastDropWarn = now
	}
	e.mu.Unlock()
	if shouldLog {
		klog.Warningf("storage export queue full (limit %d); dropping oldest samples; %d dropped so far",
			e.cfg.MaxQueueSize, e.queue.snapshotDropped())
	}
}

// worker is the write loop. Workers are identical and compete for queued
// samples; pacing and retries are identical across them.
func (e *Exporter) worker() {
	defer e.wg.Done()
	for {
		batch := e.nextBatch()
		if len(batch) == 0 {
			// Only a closed and drained queue produces an empty batch.
			return
		}
		e.writeWithRetry(batch)
	}
}

// nextBatch waits until a full batch is available, Linger elapses after the
// first queued sample, or the exporter closes. The deadline is anchored at
// the first sample seen while idle, so a slow trickle of samples still gets
// flushed promptly instead of resetting the wait on every new sample. On
// close it flushes the remaining queued samples batch by batch so Close can
// drain the backlog; an empty batch signals shutdown to the worker.
func (e *Exporter) nextBatch() []sample {
	var deadline time.Time
	for {
		depth := e.queue.len()
		if depth >= e.cfg.MaxBatchSize {
			items := e.queue.popAll()
			if len(items) == 0 {
				// Another worker drained the queue first.
				continue
			}
			if len(items) <= e.cfg.MaxBatchSize {
				return items
			}
			batch := items[:e.cfg.MaxBatchSize]
			// Re-queue the remainder at the front, preserving order for the
			// other workers.
			e.queue.pushFront(items[e.cfg.MaxBatchSize:])
			e.queue.signal()
			return batch
		}
		if depth > 0 {
			if deadline.IsZero() {
				deadline = time.Now().Add(e.cfg.Linger)
			}
			if wait := time.Until(deadline); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-e.queue.wake:
					timer.Stop()
					continue
				case <-timer.C:
				}
			}
			items := e.queue.popAll()
			if len(items) == 0 {
				// Another worker drained the queue while the timer was firing.
				deadline = time.Time{}
				continue
			}
			return items
		}
		if e.isClosed() {
			return nil
		}
		deadline = time.Time{}

		// Empty queue: wait for new data (the linger timer only runs once
		// data exists).
		select {
		case <-e.queue.wake:
		case <-e.stop:
		}
	}
}

// writeWithRetry writes one batch, retrying with exponential backoff. When
// all attempts fail the batch is discarded and its samples are counted.
// During shutdown backoff and pacing waits run to completion instead of
// being interrupted, so a batch is retried exactly MaxRetries+1 times and no
// requeue loop can occur; Close bounds the total wait with CloseTimeout.
func (e *Exporter) writeWithRetry(batch []sample) {
	e.mu.Lock()
	e.inFlight += len(batch)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.inFlight -= len(batch)
		e.mu.Unlock()
	}()

	backoff := e.cfg.InitialBackoff
	attempts := e.cfg.MaxRetries + 1

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			e.mu.Lock()
			e.retries++
			e.mu.Unlock()
			e.sleep(backoff)
			if backoff *= 2; backoff > e.cfg.MaxBackoff {
				backoff = e.cfg.MaxBackoff
			}
		}

		// A nil stop channel makes the limiter wait ignore shutdown: once
		// draining has started, in-flight batches finish their retry budget.
		var stop <-chan struct{}
		if !e.isClosed() {
			stop = e.stop
		}
		if e.lim != nil && !e.lim.wait(stop) {
			return
		}

		err := e.writer.WriteBatch(toSamples(batch))
		if err == nil {
			e.mu.Lock()
			e.written += uint64(len(batch))
			e.mu.Unlock()
			return
		}
		klog.Warningf("storage backend write failed (attempt %d/%d, batch size %d): %v", attempt+1, attempts, len(batch), err)
	}

	klog.Errorf("storage backend write failed after %d attempts; discarding %d samples", attempts, len(batch))
	e.mu.Lock()
	e.exhausted += uint64(len(batch))
	e.mu.Unlock()
}

// sleep waits for d. After Close it is uninterruptible so in-flight batches
// complete their configured retry budget; before Close it returns early on
// shutdown.
func (e *Exporter) sleep(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	if e.isClosed() {
		<-timer.C
		return
	}
	select {
	case <-timer.C:
	case <-e.stop:
	}
}

func (e *Exporter) isClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// Close stops accepting new samples, waits for workers to drain the queue
// (bounded by CloseTimeout), closes the underlying backend and reports how
// many samples remain unsent (queued plus in-flight). It is idempotent:
// repeated calls return the same result and close the backend exactly once.
func (e *Exporter) Close() (unsent int, err error) {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.mu.Unlock()
		close(e.stop)
	})

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(e.cfg.CloseTimeout):
		klog.Warningf("storage export close timed out after %v; %d samples remain unsent", e.cfg.CloseTimeout, e.unresolved())
	}

	e.closeOnce.Do(func() {
		e.closeErr = e.writer.Close()
	})
	return e.unresolved(), e.closeErr
}

// unresolved counts samples that have not been successfully written:
// everything still queued plus everything in flight.
func (e *Exporter) unresolved() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queue.len() + e.inFlight
}

// Stats returns a snapshot of the exporter counters.
func (e *Exporter) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Stats{
		Enqueued:       e.enqueued,
		QueueDropped:   e.queue.snapshotDropped(),
		RetryExhausted: e.exhausted,
		Written:        e.written,
		Retries:        e.retries,
		QueueDepth:     e.queue.len(),
		InFlight:       e.inFlight,
	}
}

// pushFront re-inserts samples at the head of the queue, preserving their
// relative order. It is used when an oversized pop is split into batches.
func (q *queue) pushFront(samples []sample) {
	if len(samples) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	for i := len(samples) - 1; i >= 0; i-- {
		if q.size == q.cap {
			// Should not normally happen (these items came from the queue),
			// but stay bounded regardless.
			q.head = (q.head + 1) % q.cap
			q.size--
			q.dropped++
			continue
		}
		q.head = (q.head - 1 + q.cap) % q.cap
		q.items[q.head] = samples[i]
		q.size++
	}
}

func toSamples(batch []sample) []Sample {
	out := make([]Sample, len(batch))
	for i, s := range batch {
		out[i] = Sample{
			ContainerInfo: s.cInfo,
			Stats:         s.stats,
		}
	}
	return out
}
