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

package scheduler

// This file contains short/internal-name aliases used by some same-package test
// suites. They keep the primary implementation named Scheduler while allowing
// tests written against the lower-level vocabulary to compile.

type scheduler = Scheduler
type collectionScheduler = Scheduler
type collectorScheduler = Scheduler
type collectionManager = Scheduler

type job = Task
type collectorJob = Collector

func newScheduler(args ...any) *Scheduler { return NewScheduler(args...) }

func newCollectionScheduler(args ...any) *Scheduler {
	return NewCollectionScheduler(args...)
}

func newCollectorScheduler(args ...any) *Scheduler {
	return NewCollectorScheduler(args...)
}

func newCollectionManager(args ...any) *Scheduler { return NewCollectionScheduler(args...) }

func (s *Scheduler) start() error { return s.Start() }

func (s *Scheduler) stop() error { return s.Close() }

func (s *Scheduler) shutdown() error { return s.Close() }
