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
	"sync"
	"time"
)

// rateLimiter is a simple token-bucket limiter shared by all exporter
// workers, so the aggregate write rate of the whole pipeline is bounded even
// when several workers are draining a large backlog. The bucket refills at
// rate tokens per second up to its burst capacity; each batch write consumes
// one token.
type rateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	last       time.Time
}

func newRateLimiter(rate float64, burst int, now time.Time) *rateLimiter {
	if rate <= 0 {
		return nil
	}
	maxTokens := float64(burst)
	if maxTokens <= 0 {
		maxTokens = 1
	}
	return &rateLimiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: rate,
		last:       now,
	}
}

// wait blocks until a token is available or stop is closed. It returns true
// when a token was consumed and false when shutting down.
func (l *rateLimiter) wait(stop <-chan struct{}) bool {
	for {
		wait := l.reserve(time.Now())
		if wait <= 0 {
			return true
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-stop:
			timer.Stop()
			return false
		}
	}
}

// reserve refills the bucket based on elapsed time and returns how long the
// caller must wait before consuming a token (0 means a token is available
// immediately).
func (l *rateLimiter) reserve(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := now.Sub(l.last).Seconds()
	l.last = now
	l.tokens += elapsed * l.refillRate
	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}
	if l.tokens >= 1 {
		l.tokens--
		return 0
	}
	// Fractional token: wait until the refill makes up the missing fraction.
	return time.Duration((1 - l.tokens) / l.refillRate * float64(time.Second))
}
