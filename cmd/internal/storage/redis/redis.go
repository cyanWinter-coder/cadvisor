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

package redis

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	info "github.com/google/cadvisor/info/v1"
	storage "github.com/google/cadvisor/lib/storage"
	"github.com/google/cadvisor/lib/storage/buffered"

	redis "github.com/gomodule/redigo/redis"
)

func init() {
	storage.RegisterStorageDriver("redis", new)
}

// Socket timeouts for redis connections. They bound how long a single batch
// write can be stuck so the exporter's retry/backoff and shutdown timeout
// stay effective against a half-open connection.
const (
	redisDialTimeout  = 5 * time.Second
	redisReadTimeout  = 10 * time.Second
	redisWriteTimeout = 10 * time.Second
)

// redisStorage marshals cAdvisor stats to JSON documents and pushes whole
// batches to Redis with a single LPUSH. The buffered.Exporter wrapping it
// provides batching, retries, queue bounds and write pacing.
type redisStorage struct {
	pool        *redis.Pool
	machineName string
	redisKey    string
}

type detailSpec struct {
	Timestamp      int64                `json:"timestamp"`
	MachineName    string               `json:"machine_name,omitempty"`
	ContainerName  string               `json:"container_Name,omitempty"`
	ContainerStats *info.ContainerStats `json:"container_stats,omitempty"`
}

func new() (storage.StorageDriver, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	writer, err := newStorage(
		hostname,
		*storage.ArgDbName,
		*storage.ArgDbHost,
		*storage.ArgBufferConcurrency,
	)
	if err != nil {
		return nil, err
	}
	return buffered.NewDriver(writer, buffered.ConfigFromFlags()), nil
}

// We must add some default params (for example: MachineName,ContainerName...)because containerStats do not include them
func (s *redisStorage) containerStatsAndDefaultValues(cInfo *info.ContainerInfo, stats *info.ContainerStats) *detailSpec {
	timestamp := stats.Timestamp.UnixNano() / 1e3
	var containerName string
	if len(cInfo.ContainerReference.Aliases) > 0 {
		containerName = cInfo.ContainerReference.Aliases[0]
	} else {
		containerName = cInfo.ContainerReference.Name
	}
	detail := &detailSpec{
		Timestamp:      timestamp,
		MachineName:    s.machineName,
		ContainerName:  containerName,
		ContainerStats: stats,
	}
	return detail
}

// WriteBatch marshals every sample of the batch and pushes them to Redis in
// one LPUSH transaction. It is safe for concurrent use: every call borrows
// its own connection from the pool.
func (s *redisStorage) WriteBatch(batch []buffered.Sample) error {
	args := make([]interface{}, 0, len(batch)+1)
	args = append(args, s.redisKey)
	for _, sample := range batch {
		if sample.Stats == nil {
			continue
		}
		b, err := json.Marshal(s.containerStatsAndDefaultValues(sample.ContainerInfo, sample.Stats))
		if err != nil {
			return fmt.Errorf("failed to marshal stats for redis: %w", err)
		}
		args = append(args, b)
	}
	if len(args) == 1 {
		return nil
	}

	conn := s.pool.Get()
	defer conn.Close()
	if _, err := conn.Do("LPUSH", args...); err != nil {
		return fmt.Errorf("failed to push stats to redis: %w", err)
	}
	return nil
}

func (s *redisStorage) Close() error {
	return s.pool.Close()
}

// Create a new redis storage driver.
// machineName: A unique identifier to identify the host that runs the current cAdvisor
// instance is running on.
// redisHost: The host which runs redis.
// redisKey: The key for the Data that stored in the redis
func newStorage(
	machineName,
	redisKey,
	redisHost string,
	concurrency int,
) (*redisStorage, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	// Connections are dialed lazily: a redis outage at startup must not
	// prevent cAdvisor from starting; the exporter retries until redis
	// comes back. Explicit timeouts are essential so that a half-open TCP
	// connection fails fast instead of pinning a worker forever and
	// defeating the exporter's close timeout.
	pool := &redis.Pool{
		MaxIdle:     concurrency,
		MaxActive:   concurrency,
		IdleTimeout: 5 * time.Minute,
		// Wait for a connection instead of failing when all are busy; the
		// exporter concurrency matches MaxActive so waits only happen during
		// brief scheduling overlap.
		Wait: true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisHost,
				redis.DialConnectTimeout(redisDialTimeout),
				redis.DialReadTimeout(redisReadTimeout),
				redis.DialWriteTimeout(redisWriteTimeout),
			)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return &redisStorage{
		pool:        pool,
		machineName: machineName,
		redisKey:    redisKey,
	}, nil
}
