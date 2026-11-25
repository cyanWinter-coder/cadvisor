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

package buffered

import (
	"fmt"
	"time"

	info "github.com/google/cadvisor/lib/model"
	"github.com/google/cadvisor/lib/storage"

	"k8s.io/klog/v2"
)

// defaultLinger keeps partial batches moving at roughly the cadence of the
// legacy storage_driver_buffer_duration flag.
var defaultLinger = 60 * time.Second

// Driver adapts an Exporter to the storage.StorageDriver interface so it can
// be registered like any other backend driver.
type Driver struct {
	exporter *Exporter
}

// NewDriver wraps writer in a buffered pipeline configured by cfg.
func NewDriver(writer BatchWriter, cfg Config) *Driver {
	return &Driver{exporter: NewExporter(writer, cfg)}
}

// Exporter exposes the underlying pipeline for inspection (counters,
// graceful close with unsent counts).
func (d *Driver) Exporter() *Exporter {
	return d.exporter
}

// AddStats enqueues the sample into the buffered pipeline.
func (d *Driver) AddStats(cInfo *info.ContainerInfo, stats *info.ContainerStats) error {
	return d.exporter.AddStats(cInfo, stats)
}

// Close drains the pipeline and closes the backend. It reports how many
// samples could not be flushed before the configured timeout: the number is
// logged and also returned as an error so callers of the plain
// storage.StorageDriver interface can surface it without a type assertion.
func (d *Driver) Close() error {
	unsent, err := d.exporter.Close()
	if unsent > 0 {
		msg := fmt.Sprintf("storage exporter closed with %d unsent samples", unsent)
		klog.Warning(msg)
		if err != nil {
			return fmt.Errorf("%s: %w", msg, err)
		}
		return fmt.Errorf("%s", msg)
	}
	return err
}

// Compile-time assertion that Driver is a StorageDriver.
var _ storage.StorageDriver = (*Driver)(nil)
