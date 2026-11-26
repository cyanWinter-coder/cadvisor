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

package http

import (
	"time"

	"github.com/google/cadvisor/cmd/internal/api"

	"github.com/prometheus/client_golang/prometheus"
)

// eventStoreCollector exposes the health of the /events store so that events
// silently lost to storage limits, slow watchers or kernel log disconnects are
// visible to operators.
type eventStoreCollector struct {
	dropped        *prometheus.Desc
	missedDelivery *prometheus.Desc
	streamGaps     *prometheus.Desc
	lastEventAge   *prometheus.Desc
}

func newEventStoreCollector() *eventStoreCollector {
	return &eventStoreCollector{
		dropped: prometheus.NewDesc(
			"cadvisor_event_store_dropped_total",
			"Cumulative number of events discarded because the event storage limit was reached. The oldest events are dropped first.",
			nil, nil),
		missedDelivery: prometheus.NewDesc(
			"cadvisor_event_stream_missed_total",
			"Cumulative number of events that could not be delivered to a watcher because its channel was full.",
			nil, nil),
		streamGaps: prometheus.NewDesc(
			"cadvisor_event_stream_gaps_total",
			"Cumulative number of intervals during which the kernel event stream was disconnected and events may have been missed.",
			nil, nil),
		lastEventAge: prometheus.NewDesc(
			"cadvisor_event_stream_last_event_age_seconds",
			"Seconds since the most recent event was recorded.",
			nil, nil),
	}
}

func (c *eventStoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.dropped
	ch <- c.missedDelivery
	ch <- c.streamGaps
	ch <- c.lastEventAge
}

func (c *eventStoreCollector) Collect(ch chan<- prometheus.Metric) {
	stats := api.EventStats()
	ch <- prometheus.MustNewConstMetric(c.dropped, prometheus.CounterValue, float64(stats.DroppedEvents))
	ch <- prometheus.MustNewConstMetric(c.missedDelivery, prometheus.CounterValue, float64(stats.MissedDelivery))
	ch <- prometheus.MustNewConstMetric(c.streamGaps, prometheus.CounterValue, float64(stats.StreamGaps))
	if !stats.LastEventTime.IsZero() {
		age := time.Since(stats.LastEventTime)
		if age < 0 {
			age = 0
		}
		ch <- prometheus.MustNewConstMetric(c.lastEventAge, prometheus.GaugeValue, age.Seconds())
	}
}
