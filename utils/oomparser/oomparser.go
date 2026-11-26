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

//go:build linux

package oomparser

import (
	"path"
	"regexp"
	"strconv"
	"time"

	"github.com/euank/go-kmsg-parser/kmsgparser"

	"k8s.io/klog/v2"
)

var (
	legacyContainerRegexp = regexp.MustCompile(`Task in (.*) killed as a result of limit of (.*)`)
	// Starting in 5.0 linux kernels, the OOM message changed
	containerRegexp = regexp.MustCompile(`oom-kill:constraint=(.*),nodemask=(.*),cpuset=(.*),mems_allowed=(.*),oom_memcg=(.*),task_memcg=(.*),task=(.*),pid=(.*),uid=(.*)`)
	lastLineRegexp  = regexp.MustCompile(`Killed process ([0-9]+) \((.+)\)`)
	firstLineRegexp = regexp.MustCompile(`invoked oom-killer:`)
)

// defaultReconnectDelay is the pause before reopening the kernel log after it
// disconnects.
const defaultReconnectDelay = 5 * time.Second

// OomParser wraps a kmsgparser in order to extract OOM events from the
// individual kernel ring buffer messages.
type OomParser struct {
	parser kmsgparser.Parser
	// newParser re-opens the kernel log. It is only set when the parser owns the
	// stream (see New), which lets StreamOoms reconnect after a disconnect.
	// Tests inject a parser with a fixed message source and leave it nil.
	newParser func() (kmsgparser.Parser, error)
	// reconnectDelay is the pause before re-opening the kernel log.
	reconnectDelay time.Duration
}

// struct that contains information related to an OOM kill instance
type OomInstance struct {
	// process id of the killed process
	Pid int
	// the name of the killed process
	ProcessName string
	// the time that the process was reported to be killed,
	// accurate to the minute
	TimeOfDeath time.Time
	// the absolute name of the container that OOMed
	ContainerName string
	// the absolute name of the container that was killed
	// due to the OOM.
	VictimContainerName string
	// the constraint that triggered the OOM.  One of CONSTRAINT_NONE,
	// CONSTRAINT_CPUSET, CONSTRAINT_MEMORY_POLICY, CONSTRAINT_MEMCG
	Constraint string
}

// StreamGap reports that the kernel event stream was interrupted between Last
// and Until, so OOM events in that interval may be missing.
type StreamGap struct {
	// Last is the time of the last kernel message read before the disconnect. It
	// is zero if no message had been read yet.
	Last time.Time
	// Until is the time at which the stream was re-established.
	Until time.Time
}

// gets the container name from a line and adds it to the oomInstance.
func getLegacyContainerName(line string, currentOomInstance *OomInstance) error {
	parsedLine := legacyContainerRegexp.FindStringSubmatch(line)
	if parsedLine == nil {
		return nil
	}
	currentOomInstance.ContainerName = path.Join("/", parsedLine[1])
	currentOomInstance.VictimContainerName = path.Join("/", parsedLine[2])
	return nil
}

// gets the container name from a line and adds it to the oomInstance.
func getContainerName(line string, currentOomInstance *OomInstance) (bool, error) {
	parsedLine := containerRegexp.FindStringSubmatch(line)
	if parsedLine == nil {
		// Fall back to the legacy format if it isn't found here.
		return false, getLegacyContainerName(line, currentOomInstance)
	}
	currentOomInstance.ContainerName = parsedLine[6]
	currentOomInstance.VictimContainerName = parsedLine[5]
	currentOomInstance.Constraint = parsedLine[1]
	pid, err := strconv.Atoi(parsedLine[8])
	if err != nil {
		return false, err
	}
	currentOomInstance.Pid = pid
	currentOomInstance.ProcessName = parsedLine[7]
	return true, nil
}

// gets the pid, name, and date from a line and adds it to oomInstance
func getProcessNamePid(line string, currentOomInstance *OomInstance) (bool, error) {
	reList := lastLineRegexp.FindStringSubmatch(line)

	if reList == nil {
		return false, nil
	}

	pid, err := strconv.Atoi(reList[1])
	if err != nil {
		return false, err
	}
	currentOomInstance.Pid = pid
	currentOomInstance.ProcessName = reList[2]
	return true, nil
}

// uses regex to see if line is the start of a kernel oom log
func checkIfStartOfOomMessages(line string) bool {
	potentialOomStart := firstLineRegexp.MatchString(line)
	return potentialOomStart
}

// StreamOoms writes to a provided a stream of OomInstance objects representing
// OOM events that are found in the logs.
// It will block and should be called from a goroutine. When the kernel log
// stream ends unexpectedly, StreamOoms reconnects and reports the interval it
// could not observe through gapStream; a nil gapStream disables that reporting.
func (p *OomParser) StreamOoms(outStream chan<- *OomInstance, gapStream chan<- StreamGap) {
	var lastMessage time.Time
	for {
		lastMessage = p.stream(outStream, lastMessage)
		if p.newParser == nil {
			// A parser with a fixed source cannot reconnect. This is the test
			// path; production parsers always set newParser.
			klog.Errorf("exiting analyzeLines. OOM events will not be reported.")
			return
		}

		// Report the blind interval so consumers can mark events in it as
		// possibly missing. Never block the stream on a slow gap consumer.
		until := time.Now()
		if gapStream != nil {
			select {
			case gapStream <- StreamGap{Last: lastMessage, Until: until}:
			default:
			}
		}
		klog.Warningf("Lost the kernel log stream; reconnecting in %v (events between %v and %v may be missing)",
			p.reconnectDelay, lastMessage, until)
		time.Sleep(p.reconnectDelay)
		for {
			parser, err := p.newParser()
			if err == nil {
				// Start at the current end of the ring buffer: replaying it would
				// re-report OOMs that were already surfaced (and that older
				// messages may have been overwritten anyway). The interval is
				// reported as a gap instead.
				if seekErr := parser.SeekEnd(); seekErr != nil {
					klog.Warningf("Unable to seek to the end of the kernel log after reconnecting: %v", seekErr)
				}
				p.parser = parser
				break
			}
			klog.Warningf("Unable to reopen the kernel log for OOM detection: %v; retrying in %v", err, p.reconnectDelay)
			time.Sleep(p.reconnectDelay)
		}
	}
}

// stream reads OOM instances from the current parser until its message channel
// closes, and returns the timestamp of the last message it saw.
func (p *OomParser) stream(outStream chan<- *OomInstance, lastMessage time.Time) time.Time {
	kmsgEntries := p.parser.Parse()
	defer p.parser.Close()

	for msg := range kmsgEntries {
		if msg.Timestamp.After(lastMessage) {
			lastMessage = msg.Timestamp
		}
		isOomMessage := checkIfStartOfOomMessages(msg.Message)
		if isOomMessage {
			oomCurrentInstance := &OomInstance{
				ContainerName:       "/",
				VictimContainerName: "/",
				TimeOfDeath:         msg.Timestamp,
			}
			for msg := range kmsgEntries {
				if msg.Timestamp.After(lastMessage) {
					lastMessage = msg.Timestamp
				}
				finished, err := getContainerName(msg.Message, oomCurrentInstance)
				if err != nil {
					klog.Errorf("%v", err)
				}
				if !finished {
					finished, err = getProcessNamePid(msg.Message, oomCurrentInstance)
					if err != nil {
						klog.Errorf("%v", err)
					}
				}
				if finished {
					oomCurrentInstance.TimeOfDeath = msg.Timestamp
					break
				}
			}
			outStream <- oomCurrentInstance
		}
	}
	return lastMessage
}

// initializes an OomParser object. Returns an OomParser object and an error.
func New() (*OomParser, error) {
	parser, err := NewParser()
	if err != nil {
		return nil, err
	}
	return &OomParser{
		parser:         parser,
		newParser:      NewParser,
		reconnectDelay: defaultReconnectDelay,
	}, nil
}

// NewParser opens the kernel log and returns a parser.
func NewParser() (kmsgparser.Parser, error) {
	parser, err := kmsgparser.NewParser()
	if err != nil {
		return nil, err
	}
	parser.SetLogger(glogAdapter{})
	return parser, nil
}

type glogAdapter struct{}

var _ kmsgparser.Logger = glogAdapter{}

func (glogAdapter) Infof(format string, args ...interface{}) {
	klog.V(4).Infof(format, args...)
}
func (glogAdapter) Warningf(format string, args ...interface{}) {
	klog.V(2).Infof(format, args...)
}
func (glogAdapter) Errorf(format string, args ...interface{}) {
	klog.Warningf(format, args...)
}
