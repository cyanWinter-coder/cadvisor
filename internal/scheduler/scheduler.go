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

// Package scheduler is a compatibility import path for the canonical
// implementation in lib/scheduler.
package scheduler

import libsched "github.com/google/cadvisor/lib/scheduler"

// Scheduler aliases the canonical scheduler.
type Scheduler = libsched.Scheduler

// Config aliases the canonical configuration.
type Config = libsched.Config

// JobID aliases the canonical job identifier.
type JobID = libsched.JobID

// Task aliases the canonical task interface.
type Task = libsched.Task

// TaskFunc aliases a task function.
type TaskFunc = libsched.TaskFunc

// NewScheduler creates a scheduler.
func NewScheduler(args ...any) *Scheduler { return libsched.NewScheduler(args...) }
