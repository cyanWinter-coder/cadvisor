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

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

// Target is an optional single-argument registration description.
type Target struct {
	ContainerID string
	Container   string

	CollectorID string
	Collector   string
	Name        string
	ID          string

	Interval time.Duration
	Timeout  time.Duration

	Task          any
	Job           any
	CollectorImpl any
}

type addSettings struct {
	container string
	collector string
	task      any
	interval  time.Duration
	timeout   time.Duration
}

func parseAddArgs(args []any) (container, collector string, task any, interval, timeout time.Duration, err error) {
	st := addSettings{}
	durations := 0

	for _, arg := range args {
		if arg == nil {
			continue
		}

		switch v := arg.(type) {
		case Target:
			mergeTarget(&st, &v)
			continue
		case *Target:
			if v != nil {
				mergeTarget(&st, v)
			}
			continue
		case JobID:
			st.container = v.Container
			st.collector = v.Collector
			continue
		case *JobID:
			if v != nil {
				st.container = v.Container
				st.collector = v.Collector
			}
			continue
		case Config:
			if v.Interval > 0 {
				st.interval = v.Interval
			}
			if v.Timeout > 0 {
				st.timeout = v.Timeout
			}
			continue
		case *Config:
			if v != nil {
				if v.Interval > 0 {
					st.interval = v.Interval
				}
				if v.Timeout > 0 {
					st.timeout = v.Timeout
				}
			}
			continue
		case Option:
			probe := &Scheduler{}
			v(probe)
			if probe.DefaultInterval > 0 {
				st.interval = probe.DefaultInterval
			}
			if probe.DefaultTimeout > 0 {
				st.timeout = probe.DefaultTimeout
			}
			continue
		case func(*Scheduler):
			probe := &Scheduler{}
			v(probe)
			if probe.DefaultInterval > 0 {
				st.interval = probe.DefaultInterval
			}
			if probe.DefaultTimeout > 0 {
				st.timeout = probe.DefaultTimeout
			}
			continue
		case time.Duration:
			switch durations {
			case 0:
				st.interval = v
			case 1:
				st.timeout = v
			}
			durations++
			continue
		case string:
			if st.container == "" {
				st.container = v
			} else if st.collector == "" {
				st.collector = v
			}
			continue
		}

		if isTaskCandidate(arg) {
			st.task = arg
			continue
		}

		// A configuration object passed to Add may carry identifiers,
		// intervals, and the task itself in fields.
		if applyGenericConfig(&st, arg); st.task == nil {
			st.task = arg
		}
	}

	if st.task == nil {
		return "", "", nil, 0, 0, fmt.Errorf("scheduler: no collector task in arguments %#v", args)
	}
	return st.container, st.collector, st.task, st.interval, st.timeout, nil
}

func mergeTarget(st *addSettings, t *Target) {
	st.container = firstNonEmpty(t.ContainerID, t.Container, st.container)
	st.collector = firstNonEmpty(t.CollectorID, t.Collector, t.Name, t.ID, st.collector)
	st.interval = firstPositiveDuration(t.Interval, st.interval)
	st.timeout = firstPositiveDuration(t.Timeout, st.timeout)
	st.task = firstNonNilTask(t.Task, t.Job, t.CollectorImpl, st.task)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstNonNilTask(values ...any) any {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func isTaskCandidate(v any) bool {
	switch v.(type) {
	case Task, Collector, MetricsTask,
		func(context.Context) error,
		func(context.Context),
		func() error,
		func(),
		func(context.Context) (any, error),
		func() (any, error):
		return true
	case nil:
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Func:
		return isCallableTaskFunc(rv.Type())
	case reflect.Ptr, reflect.Struct:
		return hasKnownTaskMethod(rv)
	default:
		return false
	}
}

func isCallableTaskFunc(ft reflect.Type) bool {
	if ft.IsVariadic() || ft.NumIn() > 1 || ft.NumOut() > 2 {
		return false
	}
	if ft.NumIn() == 1 && ft.In(0) != contextType {
		return false
	}
	if ft.NumOut() == 2 && !ft.Out(1).Implements(errorType) {
		return false
	}
	return true
}

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()

func hasKnownTaskMethod(rv reflect.Value) bool {
	for _, name := range taskMethodNames {
		m := rv.MethodByName(name)
		if m.IsValid() {
			return true
		}
	}
	return false
}

var taskMethodNames = []string{
	"Collect",
	"Run",
	"Scrape",
	"Gather",
	"CollectMetrics",
	"Execute",
	"Do",
	"Once",
}

func adaptTask(v any) (taskRunner, string, time.Duration, time.Duration, error) {
	if v == nil {
		return nil, "", 0, 0, fmt.Errorf("scheduler: nil task")
	}
	name := inferName(v)
	interval, timeout := inferTiming(v)

	switch t := v.(type) {
	case Task:
		return taskRunnerFunc(func(ctx context.Context) (any, error) { return nil, t.Run(ctx) }), name, interval, timeout, nil
	case Collector:
		return taskRunnerFunc(func(ctx context.Context) (any, error) { return nil, t.Collect(ctx) }), name, interval, timeout, nil
	case MetricsTask:
		return taskRunnerFunc(t.CollectMetrics), name, interval, timeout, nil
	case func(context.Context) error:
		return taskRunnerFunc(func(ctx context.Context) (any, error) { return nil, t(ctx) }), name, interval, timeout, nil
	case func(context.Context):
		return taskRunnerFunc(func(ctx context.Context) (any, error) { t(ctx); return nil, nil }), name, interval, timeout, nil
	case func() error:
		return taskRunnerFunc(func(context.Context) (any, error) { return nil, t() }), name, interval, timeout, nil
	case func():
		return taskRunnerFunc(func(context.Context) (any, error) { t(); return nil, nil }), name, interval, timeout, nil
	case func(context.Context) (any, error):
		return taskRunnerFunc(t), name, interval, timeout, nil
	case func() (any, error):
		return taskRunnerFunc(func(context.Context) (any, error) { return t() }), name, interval, timeout, nil
	}

	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, "", 0, 0, fmt.Errorf("scheduler: invalid task")
	}

	if rv.Kind() == reflect.Func {
		return taskRunnerFunc(func(ctx context.Context) (any, error) {
			return callReflected(rv, ctx)
		}), name, interval, timeout, nil
	}

	for _, methodName := range taskMethodNames {
		m := rv.MethodByName(methodName)
		if !m.IsValid() {
			continue
		}
		method := m
		return taskRunnerFunc(func(ctx context.Context) (any, error) {
			return callReflected(method, ctx)
		}), name, interval, timeout, nil
	}

	return nil, name, interval, timeout, fmt.Errorf("scheduler: unsupported collector type %T: expected Collect/Run method or function", v)
}

func callReflected(fn reflect.Value, ctx context.Context) (metrics any, err error) {
	ft := fn.Type()
	args := make([]reflect.Value, ft.NumIn())
	for i := 0; i < ft.NumIn(); i++ {
		pt := ft.In(i)
		switch {
		case i == 0 && pt == contextType:
			args[i] = reflect.ValueOf(ctx)
		case pt.Kind() == reflect.Map:
			args[i] = reflect.MakeMap(pt)
		case pt.Kind() == reflect.Slice && pt == reflect.TypeOf([]any{}):
			args[i] = reflect.MakeSlice(pt, 0, 0)
		default:
			args[i] = reflect.Zero(pt)
		}
	}

	var outputs []reflect.Value
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("collector panicked: %v", recovered)
			}
		}()
		outputs = fn.Call(args)
	}()
	if err != nil {
		return nil, err
	}

	var fallback any
	for _, out := range outputs {
		if !out.IsValid() {
			continue
		}
		if out.Type().Implements(errorType) {
			if !out.IsNil() {
				err = out.Interface().(error)
			}
			continue
		}
		switch out.Kind() {
		case reflect.Map:
			metrics = out.Interface()
		case reflect.Slice, reflect.Ptr:
			if metrics == nil {
				metrics = out.Interface()
			}
		case reflect.Struct:
			// The legacy collector interface returns (time.Time, metrics, err).
			// Do not mistake its first time.Time for collected metrics.
			if metrics == nil && out.Type() != reflect.TypeOf(time.Time{}) {
				fallback = out.Interface()
			}
		}
	}
	if metrics == nil {
		metrics = fallback
	}
	return metrics, err
}

func inferName(v any) string {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return ""
	}
	for _, method := range []string{"Name", "GetName", "ID", "GetID", "CollectorName", "Key"} {
		m := rv.MethodByName(method)
		if m.IsValid() && m.Type().NumIn() == 0 && m.Type().NumOut() == 1 && m.Type().Out(0).Kind() == reflect.String {
			out := m.Call(nil)
			if len(out) == 1 {
				return out[0].String()
			}
		}
	}
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return ""
	}
	for _, field := range []string{"Name", "CollectorName", "ID", "CollectorID", "Key"} {
		f := rv.FieldByName(field)
		if f.IsValid() && f.Kind() == reflect.String {
			return f.String()
		}
	}
	if name := rv.Type().Name(); name != "" {
		return name
	}
	return ""
}

func inferTiming(v any) (time.Duration, time.Duration) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return 0, 0
	}

	interval := durationMethod(rv, "Interval", "GetInterval", "Period", "GetPeriod", "PollingFrequency", "GetPollingFrequency", "CollectionInterval", "Schedule")
	timeout := durationMethod(rv, "Timeout", "GetTimeout", "CollectionTimeout", "GetCollectionTimeout", "Deadline")

	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Struct {
		if interval <= 0 {
			interval = durationField(rv, "Interval", "CollectionInterval", "Period", "PollingFrequency")
		}
		if timeout <= 0 {
			timeout = durationField(rv, "Timeout", "CollectionTimeout")
		}
	}
	return interval, timeout
}

func durationMethod(rv reflect.Value, names ...string) time.Duration {
	for _, name := range names {
		m := rv.MethodByName(name)
		if !m.IsValid() || m.Type().NumIn() != 0 || m.Type().NumOut() != 1 {
			continue
		}
		if m.Type().Out(0) != reflect.TypeOf(time.Duration(0)) {
			continue
		}
		out := m.Call(nil)
		if d, ok := out[0].Interface().(time.Duration); ok && d > 0 {
			return d
		}
	}
	return 0
}

func durationField(rv reflect.Value, names ...string) time.Duration {
	for _, name := range names {
		f := rv.FieldByName(name)
		if f.IsValid() && f.Kind() == reflect.Int64 {
			if d := time.Duration(f.Int()); d > 0 {
				return d
			}
		}
	}
	return 0
}

func applySettings(s *Scheduler, args ...any) {
	durationPos := 0
	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			continue
		case Option:
			v(s)
		case func(*Scheduler):
			v(s)
		case Config:
			if v.Interval > 0 {
				s.DefaultInterval = v.Interval
			}
			if v.CollectionInterval > 0 {
				s.DefaultInterval = v.CollectionInterval
			}
			if v.Timeout > 0 {
				s.DefaultTimeout = v.Timeout
			}
			if v.CollectorTimeout > 0 {
				s.DefaultTimeout = v.CollectorTimeout
			}
			if n := firstPositiveInt(v.MaxConcurrency, v.MaxConcurrent, v.MaxConcurrentCollectors, v.MaxWorkers, v.Workers, v.Concurrency); n > 0 {
				s.MaxConcurrency = n
			}
		case *Config:
			if v != nil {
				applySettings(s, *v)
			}
		case time.Duration:
			switch durationPos {
			case 0:
				s.DefaultInterval = v
			case 1:
				s.DefaultTimeout = v
			}
			durationPos++
		case int:
			if v > 0 {
				s.MaxConcurrency = v
			}
		default:
			applyReflectedSettings(s, v)
		}
	}
}

func applyReflectedSettings(s *Scheduler, v any) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return
	}

	if s.MaxConcurrency <= 0 {
		for _, name := range []string{"MaxConcurrency", "MaxConcurrent", "MaxConcurrentCollectors", "MaxWorkers", "Workers", "Concurrency"} {
			if f := rv.FieldByName(name); f.IsValid() {
				if n, ok := intValue(f); ok && n > 0 {
					s.MaxConcurrency = n
					break
				}
			}
		}
	}
	if s.DefaultInterval <= 0 {
		for _, name := range []string{"Interval", "DefaultInterval", "CollectionInterval", "Period"} {
			if f := rv.FieldByName(name); f.IsValid() && f.Type() == reflect.TypeOf(time.Duration(0)) {
				if d := f.Interface().(time.Duration); d > 0 {
					s.DefaultInterval = d
					break
				}
			}
		}
	}
	if s.DefaultTimeout <= 0 {
		for _, name := range []string{"Timeout", "DefaultTimeout", "CollectionTimeout"} {
			if f := rv.FieldByName(name); f.IsValid() && f.Type() == reflect.TypeOf(time.Duration(0)) {
				if d := f.Interface().(time.Duration); d > 0 {
					s.DefaultTimeout = d
					break
				}
			}
		}
	}
}

func applyGenericConfig(st *addSettings, v any) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		applyDurationFields(st, v)
		return
	}

	setString := func(target *string, names ...string) {
		if *target != "" {
			return
		}
		for _, name := range names {
			if f := rv.FieldByName(name); f.IsValid() && f.Kind() == reflect.String {
				if value := f.String(); value != "" {
					*target = value
					return
				}
			}
		}
	}
	setString(&st.container, "ContainerID", "Container", "ContainerName")
	setString(&st.collector, "CollectorID", "CollectorName", "Name", "ID")

	if st.interval <= 0 {
		st.interval = durationField(rv, "Interval", "CollectionInterval", "Period", "PollingFrequency")
	}
	if st.timeout <= 0 {
		st.timeout = durationField(rv, "Timeout", "CollectionTimeout", "CollectorTimeout")
	}

	for _, name := range []string{"Task", "Job", "CollectorImpl", "Runnable", "Handler", "Impl", "Collector"} {
		f := rv.FieldByName(name)
		if !f.IsValid() || !f.CanInterface() {
			continue
		}
		switch f.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			if f.IsNil() {
				continue
			}
		}
		candidate := f.Interface()
		if name == "Collector" {
			if _, isString := candidate.(string); isString {
				if st.collector == "" {
					st.collector = candidate.(string)
				}
				continue
			}
		}
		if isTaskCandidate(candidate) {
			st.task = candidate
			return
		}
		if st.task == nil {
			st.task = candidate
		}
	}
}

func applyDurationFields(st *addSettings, v any) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return
	}
	if st.interval <= 0 {
		st.interval = durationField(rv, "Interval", "CollectionInterval", "Period", "PollingFrequency")
	}
	if st.timeout <= 0 {
		st.timeout = durationField(rv, "Timeout", "CollectionTimeout")
	}
}

func intValue(v reflect.Value) (int, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(v.Uint()), true
	default:
		return 0, false
	}
}

func reflectValueIDs(v any) []string {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			item := rv.Index(i)
			if item.Kind() == reflect.String {
				out = append(out, item.String())
				continue
			}
			out = append(out, reflectValueIDs(item.Interface())...)
		}
		return out
	case reflect.Map:
		out := make([]string, 0, rv.Len())
		for _, key := range rv.MapKeys() {
			if key.Kind() == reflect.String {
				out = append(out, key.String())
			}
		}
		return out
	case reflect.Struct:
		for _, name := range []string{"ContainerID", "Container", "ID", "Name"} {
			if f := rv.FieldByName(name); f.IsValid() && f.Kind() == reflect.String {
				return []string{f.String()}
			}
		}
	}
	return nil
}
