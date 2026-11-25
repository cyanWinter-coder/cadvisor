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

import "time"

// Defaults shared by every buffered storage driver. They can be overridden
// through the storage_driver_buffer_* flags.
const (
	defaultMaxBatchSize   = 1000
	defaultConcurrency    = 1
	defaultMaxQueueSize   = 100_000
	defaultMaxRetries     = 5
	defaultInitialBackoff = 100 * time.Millisecond
	defaultMaxBackoff     = 30 * time.Second
	// defaultMaxBatchesPerSecond keeps a healthy backend busy while making
	// sure a recovering backend is fed smoothly instead of receiving the
	// whole backlog in one burst.
	defaultMaxBatchesPerSecond = 10
	defaultMaxBurst            = 20
	defaultCloseTimeout        = 10 * time.Second
)

func (c Config) withDefaults() Config {
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = defaultMaxBatchSize
	}
	if c.Concurrency <= 0 {
		c.Concurrency = defaultConcurrency
	}
	if c.Linger <= 0 {
		c.Linger = defaultLinger
	}
	if c.MaxQueueSize <= 0 {
		c.MaxQueueSize = defaultMaxQueueSize
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = defaultInitialBackoff
	}
	if c.MaxBackoff <= 0 || c.MaxBackoff < c.InitialBackoff {
		c.MaxBackoff = defaultMaxBackoff
	}
	if c.MaxBurst <= 0 {
		c.MaxBurst = defaultMaxBurst
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = defaultCloseTimeout
	}
	return c
}
