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
	"testing"
	"time"

	info "github.com/google/cadvisor/lib/model"
)

var errFakeBackend = errors.New("fake backend failure")

// fakeWriter records batches and can be configured to fail a number of
// writes or block until release is signaled.
type fakeWriter struct {
	mu         sync.Mutex
	failLeft   int
	batches    [][]Sample
	gate       chan struct{}
	closed     bool
	closeCount int
}

func (f *fakeWriter) WriteBatch(batch []Sample) error {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failLeft > 0 {
		f.failLeft--
		return errFakeBackend
	}
	cp := append([]Sample(nil), batch...)
	f.batches = append(f.batches, cp)
	return nil
}

func (f *fakeWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.closeCount++
	return nil
}

func (f *fakeWriter) totalWritten() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func testConfig() Config {
	return Config{
		MaxBatchSize:        10,
		Concurrency:         1,
		Linger:              20 * time.Millisecond,
		MaxQueueSize:        100,
		MaxRetries:          2,
		InitialBackoff:      time.Millisecond,
		MaxBackoff:          5 * time.Millisecond,
		MaxBatchesPerSecond: -1, // pacing disabled by default in tests
		CloseTimeout:        time.Second,
	}
}

func addSamples(t *testing.T, e *Exporter, n int) {
	t.Helper()
	cInfo := &info.ContainerInfo{ContainerReference: info.ContainerReference{Name: "c"}}
	for i := 0; i < n; i++ {
		stats := &info.ContainerStats{Timestamp: time.Unix(int64(i), 0)}
		if err := e.AddStats(cInfo, stats); err != nil {
			t.Fatalf("AddStats %d failed: %v", i, err)
		}
	}
}

func TestExporterBatchesAndDrainsOnClose(t *testing.T) {
	w := &fakeWriter{}
	e := NewExporter(w, testConfig())

	addSamples(t, e, 25)

	unsent, err := e.Close()
	if err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if unsent != 0 {
		t.Fatalf("expected full drain, %d samples unsent", unsent)
	}
	if got := w.totalWritten(); got != 25 {
		t.Fatalf("expected 25 written samples, got %d", got)
	}
	stats := e.Stats()
	if stats.Written != 25 || stats.RetryExhausted != 0 || stats.QueueDropped != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	// Batching depends on linger timing; assert the hard invariants instead
	// of an exact batch count: every batch respects MaxBatchSize and the
	// totals add up with no loss or duplication.
	if len(w.batches) == 0 {
		t.Fatal("expected at least one batch")
	}
	for i, b := range w.batches {
		if len(b) > 10 {
			t.Fatalf("batch %d exceeds MaxBatchSize: %d", i, len(b))
		}
	}
	if !w.closed {
		t.Fatal("underlying writer was not closed")
	}
}

func TestExporterRetriesThenDiscards(t *testing.T) {
	w := &fakeWriter{failLeft: 1 << 30} // every write fails
	cfg := testConfig()
	e := NewExporter(w, cfg)

	addSamples(t, e, 3)
	start := time.Now()
	unsent, _ := e.Close()
	elapsed := time.Since(start)

	// Initial attempt + MaxRetries retries, each retry preceded by a
	// backoff sleep (1ms, then 2ms, capped at MaxBackoff).
	if elapsed < cfg.InitialBackoff {
		t.Fatalf("backoff not honored: returned after %v", elapsed)
	}
	if unsent != 0 {
		t.Fatalf("discarded batches leave the queue empty, got %d unsent", unsent)
	}
	stats := e.Stats()
	if stats.RetryExhausted != 3 {
		t.Fatalf("expected 3 exhausted samples, got %d", stats.RetryExhausted)
	}
	if stats.Written != 0 || stats.Retries < uint64(cfg.MaxRetries) {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestExporterRecoversAfterTransientFailure(t *testing.T) {
	w := &fakeWriter{failLeft: 1} // first attempt fails, retry succeeds
	cfg := testConfig()
	e := NewExporter(w, cfg)

	addSamples(t, e, 4)
	unsent, _ := e.Close()

	if unsent != 0 {
		t.Fatalf("expected drain after recovery, %d unsent", unsent)
	}
	if w.totalWritten() != 4 {
		t.Fatalf("expected 4 written, got %d", w.totalWritten())
	}
	if e.Stats().Retries < 1 {
		t.Fatal("expected at least one retry to be counted")
	}
}

func TestExporterDropsOldestWhenQueueFull(t *testing.T) {
	gate := make(chan struct{})
	w := &fakeWriter{gate: gate}
	cfg := testConfig()
	cfg.MaxQueueSize = 5
	e := NewExporter(w, cfg)

	addSamples(t, e, 10)

	// The single worker may already have taken up to MaxBatchSize samples
	// in flight before the gate blocks it, so the queue can only hold 5 and
	// every other sample must have been dropped at the queue. The invariant
	// is full accounting: written + queue-dropped == enqueued.
	dropped := e.Stats().QueueDropped
	if dropped == 0 {
		t.Fatal("expected queue drops once the backend stalls")
	}
	if dropped > 5 {
		t.Fatalf("at most MaxQueueSize samples can be dropped, got %d", dropped)
	}
	close(gate)
	unsent, _ := e.Close()
	if unsent != 0 {
		t.Fatalf("expected drain, %d unsent", unsent)
	}
	if got := w.totalWritten() + int(dropped); got != 10 {
		t.Fatalf("accounting broken: written %d + dropped %d != 10", w.totalWritten(), dropped)
	}
	// FIFO: with one worker the surviving samples must arrive in timestamp
	// order.
	var prev int64 = -1
	for _, b := range w.batches {
		for _, s := range b {
			ts := s.Stats.Timestamp.Unix()
			if ts <= prev {
				t.Fatalf("samples out of order: %d after %d", ts, prev)
			}
			prev = ts
		}
	}
}

func TestExporterCloseReportsUnsentAndTimesOut(t *testing.T) {
	gate := make(chan struct{})
	w := &fakeWriter{gate: gate}
	cfg := testConfig()
	cfg.CloseTimeout = 50 * time.Millisecond
	e := NewExporter(w, cfg)

	addSamples(t, e, 7)

	start := time.Now()
	unsent, _ := e.Close()
	if time.Since(start) > 2*time.Second {
		t.Fatal("Close did not respect the timeout")
	}
	if unsent != 7 {
		t.Fatalf("expected 7 unsent samples, got %d", unsent)
	}
	// Release the stuck writer so its worker goroutine can exit.
	close(gate)
}

func TestExporterRejectsSamplesAfterClose(t *testing.T) {
	w := &fakeWriter{}
	e := NewExporter(w, testConfig())
	if _, err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.AddStats(&info.ContainerInfo{}, &info.ContainerStats{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestExporterCloseIsIdempotent(t *testing.T) {
	w := &fakeWriter{}
	e := NewExporter(w, testConfig())

	unsent1, err1 := e.Close()
	unsent2, err2 := e.Close()

	if unsent1 != 0 || unsent2 != 0 || err1 != nil || err2 != nil {
		t.Fatalf("unexpected close results: (%d,%v) (%d,%v)", unsent1, err1, unsent2, err2)
	}
	if w.closeCount != 1 {
		t.Fatalf("underlying writer must be closed exactly once, got %d", w.closeCount)
	}
}

func TestExporterConcurrentWritersNoLoss(t *testing.T) {
	w := &fakeWriter{}
	cfg := testConfig()
	cfg.Concurrency = 4
	cfg.MaxBatchSize = 7
	cfg.MaxQueueSize = 1000 // larger than the sample count so this test isolates concurrency, not overflow
	e := NewExporter(w, cfg)

	addSamples(t, e, 200)
	unsent, _ := e.Close()
	if unsent != 0 {
		t.Fatalf("expected full drain, %d unsent", unsent)
	}
	if w.totalWritten() != 200 {
		t.Fatalf("expected 200 written without loss/duplication, got %d", w.totalWritten())
	}
}

// TestExporterLingerFlushesPartialBatch verifies that a trickle of samples
// which never fills a batch is still flushed once Linger elapses after the
// first queued sample, rather than waiting forever or flushing per sample.
func TestExporterLingerFlushesPartialBatch(t *testing.T) {
	w := &fakeWriter{}
	cfg := testConfig()
	cfg.MaxBatchSize = 100
	cfg.Linger = 80 * time.Millisecond
	e := NewExporter(w, cfg)

	cInfo := &info.ContainerInfo{ContainerReference: info.ContainerReference{Name: "c"}}
	// Three samples spaced well within the linger window.
	for i := 0; i < 3; i++ {
		if err := e.AddStats(cInfo, &info.ContainerStats{Timestamp: time.Unix(int64(i), 0)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(15 * time.Millisecond)
	}
	// Wait for the linger-anchored flush without closing.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && w.totalWritten() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if w.totalWritten() != 3 {
		t.Fatalf("expected linger to flush all 3 samples in one batch, got %d written", w.totalWritten())
	}
	if len(w.batches) != 1 {
		t.Fatalf("expected one partial batch, got %d", len(w.batches))
	}
	if unsent, _ := e.Close(); unsent != 0 {
		t.Fatalf("expected clean close, %d unsent", unsent)
	}
}
