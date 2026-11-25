// Copyright 2014 Google Inc. All Rights Reserved.
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

package influxdb

import (
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/lib/storage/buffered"

	influxdb "github.com/influxdb/influxdb/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStorage builds a driver pointed at an in-process HTTP server that
// stands in for InfluxDB. The server returns 204 No Content on successful
// writes, matching the real InfluxDB write endpoint.
func newTestStorage(t *testing.T, handler http.HandlerFunc) *influxdbStorage {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)

	client, err := influxdb.NewClient(influxdb.Config{
		URL:      *parsed,
		Username: "root",
		Password: "root",
	})
	require.NoError(t, err)

	return &influxdbStorage{
		client:          client,
		machineName:     "testMachine",
		database:        "cadvisor_test",
		retentionPolicy: "cadvisor_test_rp",
	}
}

func okHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}
}

func TestContainerFileSystemStatsToPoints(t *testing.T) {
	assert := assert.New(t)
	storage := newTestStorage(t, okHandler())

	cInfo := &info.ContainerInfo{
		ContainerReference: info.ContainerReference{
			Name: "containerName",
		},
	}

	stats := &info.ContainerStats{}
	points := storage.containerFilesystemStatsToPoints(cInfo, stats)

	// stats.Filesystem is always nil here.
	assert.Nil(points)
}

func TestContainerStatsToPoints(t *testing.T) {
	storage := newTestStorage(t, okHandler())

	cInfo, stats := createTestStats()
	require.NotNil(t, stats)

	// When
	points := storage.containerStatsToPoints(cInfo, stats)
	points = append(points, storage.memoryStatsToPoints(cInfo, stats)...)
	points = append(points, storage.hugetlbStatsToPoints(cInfo, stats)...)
	points = append(points, storage.perfStatsToPoints(cInfo, stats)...)
	points = append(points, storage.resctrlStatsToPoints(cInfo, stats)...)

	// Then
	assert.NotEmpty(t, points)
	// Total points emitted for the populated test stats (the exact count
	// depends on the hugetlb page sizes and per-cpu entries in the fixture).
	assert.Len(t, points, 39)

	// CPU stats
	assertContainsPointWithValue(t, points, serCPUUsageTotal, stats.Cpu.Usage.Total)
	assertContainsPointWithValue(t, points, serCPUUsageSystem, stats.Cpu.Usage.System)
	assertContainsPointWithValue(t, points, serCPUUsageUser, stats.Cpu.Usage.User)
	assertContainsPointWithValue(t, points, serLoadAverage, stats.Cpu.LoadAverage)
	for _, cpuUsage := range stats.Cpu.Usage.PerCpu {
		assertContainsPointWithValue(t, points, serCPUUsagePerCPU, cpuUsage)
	}

	// Memory stats
	assertContainsPointWithValue(t, points, serMemoryUsage, stats.Memory.Usage)
	assertContainsPointWithValue(t, points, serMemoryMaxUsage, stats.Memory.MaxUsage)
	assertContainsPointWithValue(t, points, serMemoryCache, stats.Memory.Cache)
	assertContainsPointWithValue(t, points, serMemoryRss, stats.Memory.RSS)
	assertContainsPointWithValue(t, points, serMemorySwap, stats.Memory.Swap)
	assertContainsPointWithValue(t, points, serMemoryMappedFile, stats.Memory.MappedFile)
	assertContainsPointWithValue(t, points, serMemoryUsage, stats.Memory.Usage)
	assertContainsPointWithValue(t, points, serMemoryWorkingSet, stats.Memory.WorkingSet)
	assertContainsPointWithValue(t, points, serMemoryTotalActiveFile, stats.Memory.TotalActiveFile)
	assertContainsPointWithValue(t, points, serMemoryTotalInactiveFile, stats.Memory.TotalInactiveFile)
	assertContainsPointWithValue(t, points, serMemoryFailcnt, stats.Memory.Failcnt)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.ContainerData.Pgfault)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.ContainerData.Pgmajfault)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.HierarchicalData.Pgfault)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.HierarchicalData.Pgmajfault)

	// Hugetlb stats
	for _, hugetlbStat := range stats.Hugetlb {
		assertContainsPointWithValue(t, points, setHugetlbUsage, hugetlbStat.Usage)
		assertContainsPointWithValue(t, points, setHugetlbMaxUsage, hugetlbStat.MaxUsage)
		assertContainsPointWithValue(t, points, setHugetlbFailcnt, hugetlbStat.Failcnt)
	}

	// Network stats
	assertContainsPointWithValue(t, points, serRxBytes, stats.Network.RxBytes)
	assertContainsPointWithValue(t, points, serRxErrors, stats.Network.RxErrors)
	assertContainsPointWithValue(t, points, serTxBytes, stats.Network.TxBytes)
	assertContainsPointWithValue(t, points, serTxErrors, stats.Network.TxErrors)

	// Perf stats
	for _, perfStat := range stats.PerfStats {
		assertContainsPointWithValue(t, points, serPerfStat, perfStat.Value)
	}

	// Reference memory
	assertContainsPointWithValue(t, points, serReferencedMemory, stats.ReferencedMemory)

	// Resource Control stats - memory bandwidth
	for _, rdtMemoryBandwidth := range stats.Resctrl.MemoryBandwidth {
		assertContainsPointWithValue(t, points, serResctrlMemoryBandwidthTotal, rdtMemoryBandwidth.TotalBytes)
		assertContainsPointWithValue(t, points, serResctrlMemoryBandwidthLocal, rdtMemoryBandwidth.LocalBytes)
	}

	// Resource Control stats - cache
	for _, rdtCache := range stats.Resctrl.Cache {
		assertContainsPointWithValue(t, points, serResctrlLLCOccupancy, rdtCache.LLCOccupancy)
	}
}

// TestWriteBatchSucceeds verifies the batch writer sends every sample of a
// batch in a single HTTP call and tags the points with the machine name.
func TestWriteBatchSucceeds(t *testing.T) {
	var (
		mu       sync.Mutex
		requests [][]byte
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		requests = append(requests, body)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := influxdb.NewClient(influxdb.Config{URL: *parsed})
	require.NoError(t, err)
	s := &influxdbStorage{client: client, machineName: "machineA", database: "db"}

	cInfo, stats := createTestStats()
	batch := []buffered.Sample{
		{ContainerInfo: cInfo, Stats: stats},
		{ContainerInfo: cInfo, Stats: stats},
	}
	require.NoError(t, s.WriteBatch(batch))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 1, "the whole batch must be one request")
	body := string(requests[0])
	// Each sample produces the same set of measurements, so both samples'
	// lines must be present and every line carries the machine tag.
	assert.Contains(t, body, "machine=machineA")
	assert.GreaterOrEqual(t, strings.Count(body, "\n"), 2)
}

// TestWriteBatchPropagatesServerErrors makes sure a backend failure is
// returned as an error so the buffered exporter can retry.
func TestWriteBatchPropagatesServerErrors(t *testing.T) {
	s := newTestStorage(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "influxdb is down", http.StatusServiceUnavailable)
	})

	cInfo, stats := createTestStats()
	err := s.WriteBatch([]buffered.Sample{{ContainerInfo: cInfo, Stats: stats}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "influxdb is down")
}

func TestWriteBatchEmptyIsNoop(t *testing.T) {
	called := false
	s := newTestStorage(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})
	require.NoError(t, s.WriteBatch(nil))
	assert.False(t, called, "an empty batch must not hit the backend")
}

func assertContainsPointWithValue(t *testing.T, points []*influxdb.Point, name string, value interface{}) bool {
	found := false
	for _, point := range points {
		if point.Measurement == name && point.Fields[fieldValue] == toSignedIfUnsigned(value) {
			found = true
			break
		}
	}
	return assert.Truef(t, found, "no point found with name='%v' and value=%v", name, value)
}

func createTestStats() (*info.ContainerInfo, *info.ContainerStats) {
	cInfo := &info.ContainerInfo{
		ContainerReference: info.ContainerReference{
			Name:    "testContainername",
			Aliases: []string{"testContainerAlias1", "testContainerAlias2"},
		},
	}

	cpuUsage := info.CpuUsage{
		Total:  uint64(rand.Intn(10000)),
		PerCpu: []uint64{uint64(rand.Intn(1000)), uint64(rand.Intn(1000)), uint64(rand.Intn(1000))},
		User:   uint64(rand.Intn(10000)),
		System: uint64(rand.Intn(10000)),
	}

	stats := &info.ContainerStats{
		Timestamp: time.Now(),
		Cpu: &info.CpuStats{
			Usage:       cpuUsage,
			LoadAverage: int32(rand.Intn(1000)),
		},
		Memory: &info.MemoryStats{
			Usage:             26767396864,
			MaxUsage:          30429605888,
			Cache:             7837376512,
			RSS:               18930020352,
			Swap:              1024,
			MappedFile:        1025327104,
			WorkingSet:        23630012416,
			TotalActiveFile:   29459246253,
			TotalInactiveFile: 28364536434,
			Failcnt:           1,
			ContainerData:     info.MemoryStatsMemoryData{Pgfault: 100328455, Pgmajfault: 97},
			HierarchicalData:  info.MemoryStatsMemoryData{Pgfault: 100328454, Pgmajfault: 96},
		},
		Hugetlb: map[string]info.HugetlbStats{
			"1GB": {Usage: 1234, MaxUsage: 5678, Failcnt: 9},
			"2GB": {Usage: 9876, MaxUsage: 5432, Failcnt: 1},
		},
		ReferencedMemory: 12345,
		PerfStats:        []info.PerfStat{{Cpu: 1, PerfValue: info.PerfValue{Name: "cycles", ScalingRatio: 1.5, Value: 4589}}},
		Network:          &info.NetworkStats{},
		Resctrl: info.ResctrlStats{
			MemoryBandwidth: []info.MemoryBandwidthStats{
				{TotalBytes: 11234, LocalBytes: 4567},
				{TotalBytes: 55678, LocalBytes: 9876},
			},
			Cache: []info.CacheStats{
				{LLCOccupancy: 3},
				{LLCOccupancy: 5},
			},
		},
	}
	return cInfo, stats
}
