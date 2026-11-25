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

package manager

import sched "github.com/google/cadvisor/lib/scheduler"

// These aliases allow scheduler-focused tests that live in package manager to
// use the dependency-light canonical implementation.

type collectionScheduler = sched.Scheduler
type scheduler = sched.Scheduler

// CollectionScheduler is the exported scheduler alias.
type CollectionScheduler = sched.Scheduler

// Scheduler is an exported scheduler alias.
type Scheduler = sched.Scheduler

type collectionScheduleConfig = sched.Config
type collectionJobID = sched.JobID
type collectionTask = sched.Task

func newCollectionScheduler(args ...any) *collectionScheduler {
	return sched.NewCollectionScheduler(args...)
}

func newScheduler(args ...any) *scheduler {
	return sched.NewScheduler(args...)
}
