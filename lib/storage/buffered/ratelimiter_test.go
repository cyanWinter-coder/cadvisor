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
)

func TestRateLimiterInitialBurstThenPacing(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	start := time.Now()
	l := newRateLimiter(50, 2, start)

	// The burst bucket allows two tokens without waiting.
	if wait := l.reserve(start); wait != 0 {
		t.Fatalf("first token should be immediate, got %v", wait)
	}
	if wait := l.reserve(start); wait != 0 {
		t.Fatalf("second (burst) token should be immediate, got %v", wait)
	}

	// The bucket is empty now: the next token needs a refill of ~20ms at
	// 50 tokens/s.
	wait := l.reserve(start)
	if wait < 15*time.Millisecond || wait > 30*time.Millisecond {
		t.Fatalf("expected ~20ms refill wait, got %v", wait)
	}
	if ok := l.wait(stop); !ok {
		t.Fatal("wait interrupted unexpectedly")
	}
	if time.Since(start) < 15*time.Millisecond {
		t.Fatalf("token was granted before refill time: %v", time.Since(start))
	}
}

func TestRateLimiterDisabledForNonPositiveRate(t *testing.T) {
	if newRateLimiter(0, 1, time.Now()) != nil {
		t.Fatal("rate <= 0 should disable the limiter")
	}
}

func TestExporterPacesWrites(t *testing.T) {
	w := &fakeWriter{}
	cfg := testConfig()
	cfg.MaxBatchSize = 1         // force one write per sample so pacing is observable
	cfg.MaxBatchesPerSecond = 20 // 50ms between batch writes
	cfg.MaxBurst = 1
	e := NewExporter(w, cfg)

	addSamples(t, e, 3) // three batches on a single worker
	start := time.Now()
	if unsent, _ := e.Close(); unsent != 0 {
		t.Fatalf("expected full drain, %d unsent", unsent)
	}
	// Burst of 1 token, then two waits of ~50ms each.
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Fatalf("pacing not applied: 3 batches in %v", elapsed)
	}
}
