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

package collector

import (
	libsched "github.com/google/cadvisor/lib/scheduler"
)

// Scheduler schedules collector calls across containers with a global
// concurrency bound. It is the lifecycle-aware counterpart to the historical
// sequential GenericCollectorManager.
type Scheduler = libsched.Scheduler

type scheduler = libsched.Scheduler
type collectionScheduler = libsched.Scheduler
type collectorScheduler = libsched.Scheduler

// CollectionScheduler is a descriptive alias for Scheduler.
type CollectionScheduler = libsched.Scheduler

// CollectorScheduler is a descriptive alias for Scheduler.
type CollectorScheduler = libsched.Scheduler

// CollectionJobID identifies a collector on a container.
type CollectionJobID = libsched.JobID

// SchedulerConfig configures a Scheduler.
type SchedulerConfig = libsched.Config

// CollectionTarget is a single AddCollector registration description.
type CollectionTarget = libsched.Target

// CollectionReport describes a finished collection attempt.
type CollectionReport = libsched.Report

// ScheduledTask is a context-aware collection task.
type ScheduledTask = libsched.Task

// ScheduledTaskFunc adapts a function to ScheduledTask.
type ScheduledTaskFunc = libsched.TaskFunc

// ScheduledCollector is a context-aware collector interface.
type ScheduledCollector = libsched.Collector

// SchedulerOption configures a Scheduler.
type SchedulerOption = libsched.Option

// ErrSchedulerClosed is returned after scheduler Close.
var ErrSchedulerClosed = libsched.ErrClosed

// NewScheduler creates an unstarted collection scheduler.
func NewScheduler(args ...any) *Scheduler { return libsched.NewScheduler(args...) }

// NewCollectionScheduler creates an unstarted collection scheduler.
func NewCollectionScheduler(args ...any) *CollectionScheduler {
	return libsched.NewCollectionScheduler(args...)
}

// NewCollectorScheduler creates an unstarted collection scheduler.
func NewCollectorScheduler(args ...any) *CollectorScheduler {
	return libsched.NewCollectorScheduler(args...)
}

// NewSchedulerWithConfig creates a scheduler from typed configuration.
func NewSchedulerWithConfig(cfg SchedulerConfig) *Scheduler {
	return libsched.NewWithConfig(cfg)
}

// WithSchedulerInterval sets the default interval.
var WithSchedulerInterval = libsched.WithDefaultInterval

// WithInterval sets the default interval.
var WithInterval = libsched.WithInterval

// WithSchedulerTimeout sets the default timeout.
var WithSchedulerTimeout = libsched.WithDefaultTimeout

// WithTimeout sets the default timeout.
var WithTimeout = libsched.WithTimeout

// WithSchedulerMaxConcurrency sets the concurrency bound.
var WithSchedulerMaxConcurrency = libsched.WithMaxConcurrency

// WithMaxConcurrency sets the concurrency bound.
var WithMaxConcurrency = libsched.WithMaxConcurrency

// WithMaxWorkers sets the worker bound.
var WithMaxWorkers = libsched.WithMaxWorkers

func newScheduler(args ...any) *scheduler { return NewScheduler(args...) }

func newCollectionScheduler(args ...any) *collectionScheduler {
	return NewCollectionScheduler(args...)
}

func newCollectorScheduler(args ...any) *collectorScheduler {
	return NewCollectorScheduler(args...)
}
