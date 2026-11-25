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

// Package buffered turns an asynchronous, best-effort batch backend into a
// reliable, memory-bounded export pipeline. Producers enqueue samples through
// AddStats; a pool of worker goroutines drains the bounded FIFO queue in
// configurable batches, retries failed backend writes with exponential
// backoff, and paces the write rate so a backend coming back online is not
// flooded with the accumulated backlog.
package buffered

import (
	"sync"

	info "github.com/google/cadvisor/lib/model"
)

// sample is one queued export item: the arguments of StorageDriver.AddStats.
type sample struct {
	cInfo *info.ContainerInfo
	stats *info.ContainerStats
}

// queue is a bounded FIFO of samples. When the capacity is reached the oldest
// samples are dropped first, so a backend outage cannot make the process grow
// without limit. It is safe for concurrent use by many producers and several
// worker goroutines.
type queue struct {
	mu      sync.Mutex
	items   []sample
	head    int // index of the oldest item
	size    int // number of queued items
	cap     int
	dropped uint64 // cumulative number of samples dropped at the queue

	// wake is signaled (non-blocking send) whenever an item is enqueued.
	// Workers drain the channel and then pull everything currently available.
	wake chan struct{}
}

func newQueue(capacity int) *queue {
	return &queue{
		items: make([]sample, capacity),
		cap:   capacity,
		wake:  make(chan struct{}, 1),
	}
}

// push appends a sample, overwriting the oldest entry when full. It returns
// the number of samples dropped by this call (0 or 1).
func (q *queue) push(s sample) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	dropped := 0
	if q.size == q.cap {
		// Overwrite the oldest slot: the ring advances its head and the new
		// item lands in the slot that used to hold the oldest sample.
		q.head = (q.head + 1) % q.cap
		q.size--
		dropped = 1
		q.dropped++
	}
	tail := (q.head + q.size) % q.cap
	q.items[tail] = s
	q.size++
	return dropped
}

// signal wakes a waiting worker after one or more pushes. It never blocks:
// a pending signal already in the channel is enough for a worker to notice
// the newly enqueued data.
func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// popAll removes and returns every queued sample, oldest first. It returns
// nil when the queue is empty; the caller is expected to wait on wake().
func (q *queue) popAll() []sample {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == 0 {
		return nil
	}
	out := make([]sample, q.size)
	for i := 0; i < q.size; i++ {
		out[i] = q.items[(q.head+i)%q.cap]
	}
	q.head = 0
	q.size = 0
	return out
}

// len returns the number of currently queued samples.
func (q *queue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

// snapshotDropped returns the cumulative count of samples dropped because the
// queue was full.
func (q *queue) snapshotDropped() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}
