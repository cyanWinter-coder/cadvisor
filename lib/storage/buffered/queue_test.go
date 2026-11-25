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
	"testing"
	"time"

	info "github.com/google/cadvisor/lib/model"
)

func sampleAt(n int64) sample {
	return sample{stats: &info.ContainerStats{Timestamp: time.Unix(n, 0)}}
}

func TestQueueFIFOAndOldestDrop(t *testing.T) {
	q := newQueue(3)

	if d := q.push(sampleAt(1)); d != 0 {
		t.Fatalf("first push dropped %d samples", d)
	}
	q.push(sampleAt(2))
	q.push(sampleAt(3))

	// The queue is now full: the next push must evict the oldest sample.
	if d := q.push(sampleAt(4)); d != 1 {
		t.Fatalf("expected 1 dropped sample, got %d", d)
	}
	if got := q.snapshotDropped(); got != 1 {
		t.Fatalf("expected 1 cumulative drop, got %d", got)
	}

	got := q.popAll()
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
	want := []int64{2, 3, 4}
	for i, s := range got {
		if s.stats.Timestamp.Unix() != want[i] {
			t.Fatalf("item %d: want timestamp %d, got %d (FIFO/oldest-drop broken)", i, want[i], s.stats.Timestamp.Unix())
		}
	}

	if q.popAll() != nil {
		t.Fatal("expected nil from empty queue")
	}
}

func TestQueuePushFrontRestoresOrder(t *testing.T) {
	q := newQueue(5)
	q.push(sampleAt(1))
	q.push(sampleAt(2))

	all := q.popAll()
	// Simulate a batch that could not be sent during shutdown.
	q.pushFront(all)

	got := q.popAll()
	if len(got) != 2 || got[0].stats.Timestamp.Unix() != 1 || got[1].stats.Timestamp.Unix() != 2 {
		t.Fatalf("pushFront did not preserve order: %v", got)
	}
}
