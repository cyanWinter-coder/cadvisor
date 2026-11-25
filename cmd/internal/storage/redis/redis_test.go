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

package redis

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/lib/storage/buffered"
)

// fakeRedis is a minimal RESP server answering PING and LPUSH, enough to
// exercise the driver without a real redis deployment. It records the
// payloads of every LPUSH and can be switched into a failing mode.
type fakeRedis struct {
	ln       net.Listener
	mu       sync.Mutex
	payloads [][]byte
	fail     bool
	closed   chan struct{}
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{ln: ln, closed: make(chan struct{})}
	go f.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		<-f.closed
	})
	return f
}

func (f *fakeRedis) serve() {
	defer close(f.closed)
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeRedis) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if !strings.HasPrefix(line, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return
		}
		args := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			if _, err := r.ReadString('\n'); err != nil { // $len
				return
			}
			buf := make([]byte, 0)
			for {
				b, err := r.ReadByte()
				if err != nil {
					return
				}
				if b == '\n' && len(buf) > 0 && buf[len(buf)-1] == '\r' {
					buf = buf[:len(buf)-1]
					break
				}
				buf = append(buf, b)
			}
			args = append(args, buf)
		}

		f.mu.Lock()
		fail := f.fail
		f.mu.Unlock()
		if fail {
			_, _ = conn.Write([]byte("-ERR simulated redis failure\r\n"))
			continue
		}
		switch strings.ToUpper(string(args[0])) {
		case "PING":
			_, _ = conn.Write([]byte("+PONG\r\n"))
		case "LPUSH":
			// args[0]=LPUSH, args[1]=key, args[2:]=values.
			f.mu.Lock()
			f.payloads = append(f.payloads, args[2:]...)
			f.mu.Unlock()
			_, _ = conn.Write([]byte(":" + strconv.Itoa(len(args)-1) + "\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

func (f *fakeRedis) addr() string { return f.ln.Addr().String() }

func (f *fakeRedis) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.payloads)
}

func (f *fakeRedis) setFail(v bool) {
	f.mu.Lock()
	f.fail = v
	f.mu.Unlock()
}

func testSample() (buffered.Sample, *info.ContainerStats) {
	cInfo := &info.ContainerInfo{
		ContainerReference: info.ContainerReference{
			Name:    "c1",
			Aliases: []string{"alias1"},
		},
	}
	stats := &info.ContainerStats{Timestamp: time.Unix(123, 0)}
	return buffered.Sample{ContainerInfo: cInfo, Stats: stats}, stats
}

func TestRedisWriteBatchSingleLPUSH(t *testing.T) {
	srv := newFakeRedis(t)
	s, err := newStorage("machineA", "cadvisor", srv.addr(), 2)
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	defer s.Close()

	sample1, _ := testSample()
	sample2, _ := testSample()
	if err := s.WriteBatch([]buffered.Sample{sample1, sample2}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if got := srv.count(); got != 2 {
		t.Fatalf("expected 2 LPUSH payloads, got %d", got)
	}
}

func TestRedisWriteBatchFailureIsRetryableError(t *testing.T) {
	srv := newFakeRedis(t)
	s, err := newStorage("machineA", "cadvisor", srv.addr(), 1)
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	defer s.Close()

	srv.setFail(true)
	sample, _ := testSample()
	err = s.WriteBatch([]buffered.Sample{sample})
	if err == nil || !strings.Contains(err.Error(), "simulated redis failure") {
		t.Fatalf("expected backend error to propagate, got %v", err)
	}

	// Recovery: the pool dials fresh connections and writes succeed again.
	srv.setFail(false)
	if err := s.WriteBatch([]buffered.Sample{sample}); err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
}

func TestRedisDriverThroughBufferedPipeline(t *testing.T) {
	srv := newFakeRedis(t)
	writer, err := newStorage("machineA", "cadvisor", srv.addr(), 1)
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	driver := buffered.NewDriver(writer, buffered.Config{
		MaxBatchSize: 3,
		Linger:       20 * time.Millisecond,
		MaxRetries:   1,
		CloseTimeout: time.Second,
	})

	sample, _ := testSample()
	for i := 0; i < 7; i++ {
		if err := driver.AddStats(sample.ContainerInfo, sample.Stats); err != nil {
			t.Fatalf("AddStats: %v", err)
		}
	}
	if err := driver.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := srv.count(); got != 7 {
		t.Fatalf("expected all 7 samples delivered, got %d", got)
	}
}

func TestRedisNewStorageRejectsBadAddress(t *testing.T) {
	// Dialing is lazy, so construction succeeds even for an unreachable host;
	// writes must surface the dial error instead of panicking.
	s, err := newStorage("machineA", "cadvisor", "127.0.0.1:1", 1)
	if err != nil {
		t.Fatalf("newStorage should not dial eagerly, got %v", err)
	}
	defer s.Close()

	sample, _ := testSample()
	if err := s.WriteBatch([]buffered.Sample{sample}); err == nil {
		t.Fatal("expected a dial/connection error")
	}
}
