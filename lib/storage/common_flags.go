// Copyright 2015 Google Inc. All Rights Reserved.
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

package storage

import (
	"flag"
	"time"
)

var ArgDbUsername = flag.String("storage_driver_user", "root", "database username")
var ArgDbPassword = flag.String("storage_driver_password", "root", "database password")
var ArgDbHost = flag.String("storage_driver_host", "localhost:8086", "database host:port")
var ArgDbName = flag.String("storage_driver_db", "cadvisor", "database name")
var ArgDbTable = flag.String("storage_driver_table", "stats", "table name")
var ArgDbIsSecure = flag.Bool("storage_driver_secure", false, "use secure connection with database")
var ArgDbBufferDuration = flag.Duration("storage_driver_buffer_duration", 60*time.Second, "Writes in the storage driver will be buffered for this duration, and committed to the non memory backends as a single transaction")

// Buffered export pipeline flags. They apply to every storage driver built
// on top of lib/storage/buffered (currently influxdb and redis).
var (
	ArgBufferBatchSize           = flag.Int("storage_driver_buffer_batch_size", 1000, "Maximum number of samples sent to a storage backend in one batch write")
	ArgBufferConcurrency         = flag.Int("storage_driver_buffer_concurrency", 1, "Number of concurrent batch writers per storage backend")
	ArgBufferQueueSize           = flag.Int("storage_driver_buffer_queue_size", 100000, "Maximum number of samples buffered in memory per storage backend. When the queue is full the oldest samples are dropped first")
	ArgBufferMaxRetries          = flag.Int("storage_driver_buffer_max_retries", 5, "Number of retries for a failed backend batch write before the batch is discarded and counted")
	ArgBufferInitialBackoff      = flag.Duration("storage_driver_buffer_initial_backoff", 100*time.Millisecond, "Initial backoff before retrying a failed backend batch write; the delay doubles after every retry up to storage_driver_buffer_max_backoff")
	ArgBufferMaxBackoff          = flag.Duration("storage_driver_buffer_max_backoff", 30*time.Second, "Maximum backoff between retries of a failed backend batch write")
	ArgBufferMaxBatchesPerSecond = flag.Float64("storage_driver_buffer_max_batches_per_second", 10, "Maximum aggregate batch writes per second per storage backend, used to feed a recovering backend smoothly instead of bursting the whole backlog at it. <= 0 disables the limit")
	ArgBufferMaxBurst            = flag.Int("storage_driver_buffer_max_burst", 20, "Token-bucket burst size for storage_driver_buffer_max_batches_per_second")
	ArgBufferCloseTimeout        = flag.Duration("storage_driver_buffer_close_timeout", 10*time.Second, "Maximum time a storage driver waits to flush the buffered backlog on shutdown")
)
