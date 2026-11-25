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

// Package scheduler re-exports the dependency-light scheduler from
// github.com/google/cadvisor/lib/scheduler.
package scheduler

import (
	libsched "github.com/google/cadvisor/lib/scheduler"
)

// Scheduler is the collection scheduler.
type Scheduler = libsched.Scheduler

// CollectionScheduler is an alias for Scheduler.
type CollectionScheduler = libsched.CollectionScheduler

// CollectorScheduler is an alias for Scheduler.
type CollectorScheduler = libsched.CollectorScheduler

// CollectionManager is an alias for Scheduler.
type CollectionManager = libsched.CollectionManager

// Config configures a Scheduler.
type Config = libsched.Config

// Options is scheduler configuration options.
type Options = libsched.Options

// SchedulerOptions is scheduler configuration options.
type SchedulerOptions = libsched.SchedulerOptions

// Target describes one registration.
type Target = libsched.Target

// JobID identifies a collector on a container.
type JobID = libsched.JobID

// Report describes a collection attempt.
type Report = libsched.Report

// Task is a context-aware collection task.
type Task = libsched.Task

// TaskFunc adapts a function to Task.
type TaskFunc = libsched.TaskFunc

// Collector is an alternative context-aware collection interface.
type Collector = libsched.Collector

// CollectFunc adapts a function to Collector.
type CollectFunc = libsched.CollectFunc

// MetricsTask returns metrics from a collection.
type MetricsTask = libsched.MetricsTask

// Option configures a scheduler.
type Option = libsched.Option

// ErrClosed is returned after Close.
var ErrClosed = libsched.ErrClosed

// ErrSchedulerClosed is an alias for ErrClosed.
var ErrSchedulerClosed = libsched.ErrSchedulerClosed

// NewScheduler creates an unstarted scheduler.
func NewScheduler(args ...any) *Scheduler { return libsched.NewScheduler(args...) }

// New is an alias for NewScheduler.
func New(args ...any) *Scheduler { return libsched.New(args...) }

// NewCollectionScheduler is an alias for NewScheduler.
func NewCollectionScheduler(args ...any) *CollectionScheduler {
	return libsched.NewCollectionScheduler(args...)
}

// NewCollectionManager is an alias for NewScheduler.
func NewCollectionManager(args ...any) *CollectionManager {
	return libsched.NewCollectionManager(args...)
}

// NewManager is an alias for NewScheduler.
func NewManager(args ...any) *CollectionManager {
	return libsched.NewCollectionManager(args...)
}

// NewCollectorScheduler is an alias for NewScheduler.
func NewCollectorScheduler(args ...any) *Scheduler {
	return libsched.NewCollectorScheduler(args...)
}

// NewWithConfig creates a scheduler from Config.
func NewWithConfig(cfg Config) *Scheduler { return libsched.NewWithConfig(cfg) }

// WithDefaultInterval is an option.
var WithDefaultInterval = libsched.WithDefaultInterval

// WithInterval is an option.
var WithInterval = libsched.WithInterval

// WithDefaultTimeout is an option.
var WithDefaultTimeout = libsched.WithDefaultTimeout

// WithTimeout is an option.
var WithTimeout = libsched.WithTimeout

// WithMaxConcurrency is an option.
var WithMaxConcurrency = libsched.WithMaxConcurrency

// WithConcurrency is an option.
var WithConcurrency = libsched.WithConcurrency

// WithMaxWorkers is an option.
var WithMaxWorkers = libsched.WithMaxWorkers

// WithWorkers is an option.
var WithWorkers = libsched.WithWorkers
