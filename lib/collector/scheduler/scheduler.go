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

// Package scheduler re-exports the canonical scheduler for callers that expect
// it beneath collector/.
package scheduler

import sched "github.com/google/cadvisor/lib/scheduler"

// Scheduler is the collection scheduler.
type Scheduler = sched.Scheduler

// CollectionScheduler is an alias for Scheduler.
type CollectionScheduler = sched.Scheduler

// Config is scheduler configuration.
type Config = sched.Config

// JobID identifies a collector on a container.
type JobID = sched.JobID

// Target is a registration description.
type Target = sched.Target

// Task is a context-aware collection task.
type Task = sched.Task

// TaskFunc adapts a function to Task.
type TaskFunc = sched.TaskFunc

// Collector is a context-aware collector interface.
type Collector = sched.Collector

// NewScheduler creates a scheduler.
func NewScheduler(args ...any) *Scheduler { return sched.NewScheduler(args...) }

// NewCollectionScheduler creates a scheduler.
func NewCollectionScheduler(args ...any) *CollectionScheduler {
	return sched.NewCollectionScheduler(args...)
}
