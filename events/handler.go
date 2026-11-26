// Copyright 2015 Google Inc. All Rights Reserved.
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

package events

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/lib/utils"

	"k8s.io/klog/v2"
)

type byTimestamp []*info.Event

// functions necessary to implement the sort interface on the Events struct
func (e byTimestamp) Len() int {
	return len(e)
}

func (e byTimestamp) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e byTimestamp) Less(i, j int) bool {
	return e[i].Timestamp.Before(e[j].Timestamp)
}

type EventChannel struct {
	// Watch ID. Can be used by the caller to request cancellation of watch events.
	watchID int
	// Channel on which the caller can receive watch events.
	channel chan *info.Event
}

// Request holds a set of parameters by which Event objects may be screened.
// The caller may want events that occurred within a specific timeframe
// or of a certain type, which may be specified in the *Request object
// they pass to an EventManager function
type Request struct {
	// events falling before StartTime do not satisfy the request. StartTime
	// must be left blank in calls to WatchEvents
	StartTime time.Time
	// events falling after EndTime do not satisfy the request. EndTime
	// must be left blank in calls to WatchEvents
	EndTime time.Time
	// EventType is a map that specifies the type(s) of events wanted
	EventType map[info.EventType]bool
	// allows the caller to put a limit on how many
	// events to receive. If there are more events than MaxEventsReturned
	// then the most chronologically recent events in the time period
	// specified are returned. Must be >= 1
	MaxEventsReturned int
	// the absolute container name for which the event occurred
	ContainerName string
	// if IncludeSubcontainers is false, only events occurring in the specific
	// container, and not the subcontainers, will be returned
	IncludeSubcontainers bool
}

// EventManager is implemented by Events. It provides two ways to monitor
// events and one way to add events
type EventManager interface {
	// WatchEvents() allows a caller to register for receiving events based on the specified request.
	// On successful registration, an EventChannel object is returned.
	WatchEvents(request *Request) (*EventChannel, error)
	// GetEvents() returns all detected events based on the filters specified in request.
	GetEvents(request *Request) ([]*info.Event, error)
	// AddEvent allows the caller to add an event to an EventManager
	// object
	AddEvent(event *info.Event) error
	// Cancels a previously requested watch event.
	StopWatch(watchID int)
}

// Stats describes the health of the event store.
type Stats struct {
	// DroppedEvents is the cumulative number of events discarded because the
	// storage limit was exceeded. The oldest events are dropped first.
	DroppedEvents uint64
	// MissedDelivery is the number of events that could not be handed to a
	// watcher because its channel was full.
	MissedDelivery uint64
	// StreamGaps is the number of gaps recorded in the event stream.
	StreamGaps uint64
	// LastEventTime is the timestamp of the most recent event added.
	LastEventTime time.Time
}

// StatsProvider is implemented by managers that can report event store health.
// It is separate from EventManager so that existing implementations and
// callers of EventManager keep working unchanged.
type StatsProvider interface {
	Stats() Stats
}

// StreamGapRecorder is implemented by managers that can record intervals
// during which the event source was disconnected. It is separate from
// EventManager so that existing implementations keep satisfying it.
type StreamGapRecorder interface {
	AddStreamGap(since, until time.Time) error
}

// aggregateKey identifies the events that may be folded together. Including the
// container identity keeps events from different incarnations of a reused
// container name apart.
type aggregateKey struct {
	eventType info.EventType
	container string
	identity  string
}

// events provides an implementation for the EventManager interface.
type events struct {
	// eventStore holds the events by event type.
	eventStore map[info.EventType]*utils.TimedStore
	// map of registered watchers keyed by watch id.
	watchers map[int]*watch
	// lock guarding the eventStore.
	eventsLock sync.RWMutex
	// lock guarding watchers.
	watcherLock sync.RWMutex
	// last allocated watch id.
	lastID int
	// Event storage policy.
	storagePolicy StoragePolicy
	// aggregator folds repeated occurrences into stored events.
	aggregator *aggregator
	// lastEventTime is the time of the most recent occurrence accepted by the
	// manager, used to detect gaps in the event stream.
	lastEventTime time.Time
	// droppedEvents counts events discarded by the storage limits.
	droppedEvents atomic.Uint64
	// missedDelivery counts events that could not be handed to a full watcher.
	missedDelivery atomic.Uint64
	// streamGaps counts the gaps recorded in the event stream.
	streamGaps atomic.Uint64
}

// initialized by a call to WatchEvents(), a watch struct will then be added
// to the events slice of *watch objects. When AddEvent() finds an event that
// satisfies the request parameter of a watch object in events.watchers,
// it will send that event out over the watch object's channel. The caller that
// called WatchEvents will receive the event over the channel provided to
// WatchEvents
type watch struct {
	// request parameters passed in by the caller of WatchEvents()
	request *Request
	// a channel used to send event back to the caller.
	eventChannel *EventChannel
}

func NewEventChannel(watchID int) *EventChannel {
	return &EventChannel{
		watchID: watchID,
		channel: make(chan *info.Event, 10),
	}
}

// StoragePolicy specifies how many events to store.
// MaxAge is the max duration for which to keep events.
// MaxNumEvents is the max number of events to keep (-1 for no limit).
type StoragePolicy struct {
	// Defaults limites, used if a per-event limit is not set.
	DefaultMaxAge       time.Duration
	DefaultMaxNumEvents int

	// Per-event type limits.
	PerTypeMaxAge       map[info.EventType]time.Duration
	PerTypeMaxNumEvents map[info.EventType]int

	// AggregationWindow is the maximum gap between two occurrences of the same
	// event (same container identity and event type) that are folded into a
	// single stored event. A zero window disables aggregation.
	AggregationWindow time.Duration
}

func DefaultStoragePolicy() StoragePolicy {
	return StoragePolicy{
		DefaultMaxAge:       24 * time.Hour,
		DefaultMaxNumEvents: 100000,
		PerTypeMaxAge:       make(map[info.EventType]time.Duration),
		PerTypeMaxNumEvents: make(map[info.EventType]int),
		AggregationWindow:   time.Minute,
	}
}

// returns a pointer to an initialized Events object.
func NewEventManager(storagePolicy StoragePolicy) EventManager {
	return &events{
		eventStore:    make(map[info.EventType]*utils.TimedStore),
		watchers:      make(map[int]*watch),
		storagePolicy: storagePolicy,
		aggregator:    newAggregator(storagePolicy.AggregationWindow),
	}
}

// returns a pointer to an initialized Request object
func NewRequest() *Request {
	return &Request{
		EventType:            map[info.EventType]bool{},
		IncludeSubcontainers: false,
		MaxEventsReturned:    10,
	}
}

// returns a pointer to an initialized watch object
func newWatch(request *Request, eventChannel *EventChannel) *watch {
	return &watch{
		request:      request,
		eventChannel: eventChannel,
	}
}

func (ch *EventChannel) GetChannel() chan *info.Event {
	return ch.channel
}

func (ch *EventChannel) GetWatchId() int {
	return ch.watchID
}

// sorts and returns up to the last MaxEventsReturned chronological elements
func getMaxEventsReturned(request *Request, eSlice []*info.Event) []*info.Event {
	sort.Sort(byTimestamp(eSlice))
	n := request.MaxEventsReturned
	if n >= len(eSlice) || n <= 0 {
		return eSlice
	}
	return eSlice[len(eSlice)-n:]
}

// If the request wants all subcontainers, this returns if the request's
// container path is a prefix of the event container path.  Otherwise,
// it checks that the container paths of the event and request are
// equivalent
func isSubcontainer(request *Request, event *info.Event) bool {
	if request.IncludeSubcontainers {
		return request.ContainerName == "/" || strings.HasPrefix(event.ContainerName+"/", request.ContainerName+"/")
	}
	return event.ContainerName == request.ContainerName
}

// determines if an event occurs within the time set in the request object and is the right type.
// An aggregated event is matched by its first occurrence (Timestamp), the same
// instant the event store indexed it under; its LastTimestamp only records how
// recently it recurred.
func checkIfEventSatisfiesRequest(request *Request, event *info.Event) bool {
	startTime := request.StartTime
	endTime := request.EndTime
	eventTime := event.Timestamp
	if !startTime.IsZero() {
		if startTime.After(eventTime) {
			return false
		}
	}
	if !endTime.IsZero() {
		if endTime.Before(eventTime) {
			return false
		}
	}
	if !request.EventType[event.EventType] {
		return false
	}
	// A stream gap is host-wide: it affects every container's event stream, so
	// it is reported to callers filtering on any container.
	if event.EventType == info.EventStreamGap {
		return true
	}
	if request.ContainerName != "" {
		return isSubcontainer(request, event)
	}
	return true
}

// method of Events object that screens Event objects found in the eventStore
// attribute and if they fit the parameters passed by the Request object,
// adds it to a slice of *Event objects that is returned. If both MaxEventsReturned
// and StartTime/EndTime are specified in the request object, then only
// up to the most recent MaxEventsReturned events in that time range are returned.
func (e *events) GetEvents(request *Request) ([]*info.Event, error) {
	returnEventList := []*info.Event{}
	e.eventsLock.RLock()
	defer e.eventsLock.RUnlock()
	for eventType, fetch := range request.EventType {
		if !fetch {
			continue
		}
		evs, ok := e.eventStore[eventType]
		if !ok {
			continue
		}

		res := evs.InTimeRange(request.StartTime, request.EndTime, request.MaxEventsReturned)
		for _, in := range res {
			e := in.(*info.Event)
			if checkIfEventSatisfiesRequest(request, e) {
				returnEventList = append(returnEventList, e)
			}
		}
	}
	returnEventList = getMaxEventsReturned(request, returnEventList)
	return returnEventList, nil
}

// method of Events object that maintains an *Event channel passed by the user.
// When an event is added by AddEvents that satisfies the parameters in the passed
// Request object it is fed to the channel. The StartTime and EndTime of the watch
// request should be uninitialized because the purpose is to watch indefinitely
// for events that will happen in the future
func (e *events) WatchEvents(request *Request) (*EventChannel, error) {
	if !request.StartTime.IsZero() || !request.EndTime.IsZero() {
		return nil, errors.New(
			"for a call to watch, request.StartTime and request.EndTime must be uninitialized")
	}
	e.watcherLock.Lock()
	defer e.watcherLock.Unlock()
	newID := e.lastID + 1
	returnEventChannel := NewEventChannel(newID)
	newWatcher := newWatch(request, returnEventChannel)
	e.watchers[newID] = newWatcher
	e.lastID = newID
	return returnEventChannel, nil
}

// helper function to update the event manager's eventStore. The caller must
// hold eventsLock. It reports whether the event was stored at all.
func (e *events) storeEvent(event *info.Event) bool {
	if _, ok := e.eventStore[event.EventType]; !ok {
		maxNumEvents := e.storagePolicy.DefaultMaxNumEvents
		if numEvents, ok := e.storagePolicy.PerTypeMaxNumEvents[event.EventType]; ok {
			maxNumEvents = numEvents
		}
		if maxNumEvents == 0 {
			// Event storage is disabled for event.EventType
			return false
		}

		maxAge := e.storagePolicy.DefaultMaxAge
		if age, ok := e.storagePolicy.PerTypeMaxAge[event.EventType]; ok {
			maxAge = age
		}

		e.eventStore[event.EventType] = utils.NewTimedStore(maxAge, maxNumEvents)
	}
	dropped := e.eventStore[event.EventType].Add(event.Timestamp, event)
	if len(dropped) > 0 {
		total := e.droppedEvents.Add(uint64(len(dropped)))
		e.aggregator.forget(dropped)
		klog.V(4).Infof("Dropped %d old %s events: storage limit %d reached (%d dropped so far)",
			len(dropped), event.EventType, e.storagePolicy.limitFor(event.EventType), total)
	}
	return true
}

func (p StoragePolicy) limitFor(eventType info.EventType) int {
	if limit, ok := p.PerTypeMaxNumEvents[eventType]; ok {
		return limit
	}
	return p.DefaultMaxNumEvents
}

// updateEventStore records event, folding it into a recently stored equivalent
// event when possible. It returns the event as stored: either the pre-existing
// aggregated event (whose Count and LastTimestamp were updated) or a copy of
// event appended to the store. The caller must hold eventsLock.
func (e *events) updateEventStore(event *info.Event) *info.Event {
	key, ok := newAggregateKey(event)
	if !ok || e.aggregator.window <= 0 {
		e.storeEvent(event)
		return event
	}

	if existing := e.aggregator.lookup(key, event.Timestamp); existing != nil {
		// Replace the stored copy rather than mutating it: GetEvents may hand
		// the same pointer to callers that encode it concurrently.
		updated := *existing
		updated.Count++
		updated.LastTimestamp = event.Timestamp
		// The aggregated event describes the most recent occurrence.
		updated.EventData = event.EventData
		store := e.eventStore[event.EventType]
		if store != nil && store.Replace(existing, &updated) {
			e.aggregator.remember(key, &updated, event.Timestamp)
			return &updated
		}
		// The stored event is gone (racy or disabled storage); fall through and
		// record this occurrence as a fresh event.
		e.aggregator.forgetKey(key)
	}

	// Store a copy so the caller's event (which may be delivered to watchers) is
	// not mutated as later occurrences are folded in.
	stored := *event
	stored.Count = 1
	if !e.storeEvent(&stored) {
		// Storage is disabled for this event type; nothing to aggregate into.
		return event
	}
	e.aggregator.remember(key, &stored, event.Timestamp)
	return &stored
}

// AddStreamGap records that the kernel event stream was disconnected between
// since and until, so events in that interval may be missing. It is delivered
// to watchers that request EventStreamGap.
func (e *events) AddStreamGap(since, until time.Time) error {
	event := &info.Event{
		ContainerName: "/",
		Timestamp:     since,
		LastTimestamp: until,
		EventType:     info.EventStreamGap,
		EventData: info.EventData{
			StreamGap: &info.StreamGapEventData{Since: since, Until: until},
		},
	}
	e.eventsLock.Lock()
	stored := e.updateEventStore(event)
	e.streamGaps.Add(1)
	if until.After(e.lastEventTime) {
		e.lastEventTime = until
	}
	e.eventsLock.Unlock()
	e.publish(stored)
	return nil
}

// Stats reports the health of the event store.
func (e *events) Stats() Stats {
	e.eventsLock.RLock()
	defer e.eventsLock.RUnlock()
	stats := Stats{
		DroppedEvents:  e.droppedEvents.Load(),
		MissedDelivery: e.missedDelivery.Load(),
		StreamGaps:     e.streamGaps.Load(),
		LastEventTime:  e.lastEventTime,
	}
	return stats
}

func (e *events) findValidWatchers(event *info.Event) []*watch {
	watchesToSend := make([]*watch, 0)
	for _, watcher := range e.watchers {
		watchRequest := watcher.request
		if checkIfEventSatisfiesRequest(watchRequest, event) {
			watchesToSend = append(watchesToSend, watcher)
		}
	}
	return watchesToSend
}

// method of Events object that adds the argument Event object to the
// eventStore. It also feeds the event to a set of watch channels
// held by the manager if it satisfies the request keys of the channels
func (e *events) AddEvent(event *info.Event) error {
	e.eventsLock.Lock()
	e.updateEventStore(event)
	if event.Timestamp.After(e.lastEventTime) {
		e.lastEventTime = event.Timestamp
	}
	e.eventsLock.Unlock()
	// Watchers see each occurrence as it happens; the aggregated view is what
	// GetEvents returns.
	e.publish(event)
	klog.V(4).Infof("Added event %v", event)
	return nil
}

// publish delivers event to every watcher whose request it satisfies. A
// watcher whose channel is full misses the event rather than blocking the
// caller; the miss is counted and reported through Stats.
func (e *events) publish(event *info.Event) {
	e.watcherLock.RLock()
	defer e.watcherLock.RUnlock()
	for _, watchObject := range e.findValidWatchers(event) {
		select {
		case watchObject.eventChannel.GetChannel() <- event:
		default:
			e.missedDelivery.Add(1)
			klog.V(4).Infof("Dropped event %v for watcher %d: channel full", event, watchObject.eventChannel.GetWatchId())
		}
	}
}

// Removes a watch instance from the EventManager's watchers map
func (e *events) StopWatch(watchID int) {
	e.watcherLock.Lock()
	defer e.watcherLock.Unlock()
	_, ok := e.watchers[watchID]
	if !ok {
		klog.Errorf("Could not find watcher instance %v", watchID)
	}
	close(e.watchers[watchID].eventChannel.GetChannel())
	delete(e.watchers, watchID)
}
