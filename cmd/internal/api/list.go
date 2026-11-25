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

package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	info "github.com/google/cadvisor/info/v1"
)

// Query parameters understood by the hardened list endpoints. They deliberately
// follow the two most common pagination conventions so existing front ends
// can use whichever they already speak: limit/offset and page/page_size.
const (
	ParamLimit    = "limit"
	ParamOffset   = "offset"
	ParamPage     = "page"
	ParamPageSize = "page_size"
	ParamPerPage  = "per_page"
	ParamFields   = "fields"
	ParamFull     = "full"
	ParamView     = "view"

	ViewSlim = "slim"
	ViewFull = "full"
)

// defaultPageSize is used when page/page_size pagination is requested without
// an explicit size. limit/offset requests without a limit stay unbounded to
// preserve the historical behavior of the API.
const defaultPageSize = 100

// ListOptions controls pagination and field projection for container list
// endpoints. The zero value returns every entry in its default (slim) form.
type ListOptions struct {
	// Limit is the maximum number of entries to return. A value <= 0 means
	// "no limit".
	Limit int

	// Offset is the number of entries to skip before returning results.
	Offset int

	// Page is the optional 1-based page number. When set (and Offset was not
	// given explicitly) it is converted to Offset using PageSize.
	Page int

	// PageSize is used together with Page. It aliases Limit when pagination is
	// page based.
	PageSize int

	// Fields is the explicit allow-list of JSON fields to return. Field names
	// are JSON keys and dotted paths (e.g. "spec.image") are supported.
	Fields []string

	// Full requests the complete historical payload instead of the slim
	// default. Fields can still be used to project the full payload.
	Full bool
}

// ListResponse is the envelope used by paginated list endpoints.
type ListResponse[T any] struct {
	Items  []T `json:"items"`
	Total  int `json:"total"`
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset"`
	Page   int `json:"page,omitempty"`

	// Pages is the total number of pages when page based pagination was used.
	Pages int `json:"pages,omitempty"`
}

// ParseListOptions extracts pagination/projection options from an HTTP request.
// Malformed numeric values are ignored rather than rejected: the API treats
// unknown or invalid tuning parameters as "unset", matching the tolerant
// parsing already used for event options. Use ParseListOptionsChecked if
// invalid parameters should be reported to the client.
func ParseListOptions(r *http.Request) ListOptions {
	opts, _ := ParseListOptionsChecked(r)
	return opts
}

// ParseListOptionsChecked is ParseListOptions but returns an error describing
// the first invalid numeric parameter.
func ParseListOptionsChecked(r *http.Request) (ListOptions, error) {
	q := r.URL.Query()
	opts := ListOptions{}

	var err error
	if v := q.Get(ParamLimit); v != "" {
		if opts.Limit, err = strconv.Atoi(v); err != nil {
			return opts, errInvalidParam(ParamLimit, v)
		}
	}
	if v := q.Get(ParamPageSize); v != "" {
		if opts.PageSize, err = strconv.Atoi(v); err != nil {
			return opts, errInvalidParam(ParamPageSize, v)
		}
	} else if v := q.Get(ParamPerPage); v != "" {
		if opts.PageSize, err = strconv.Atoi(v); err != nil {
			return opts, errInvalidParam(ParamPerPage, v)
		}
	}
	if v := q.Get(ParamOffset); v != "" {
		if opts.Offset, err = strconv.Atoi(v); err != nil {
			return opts, errInvalidParam(ParamOffset, v)
		}
	}
	if v := q.Get(ParamPage); v != "" {
		if opts.Page, err = strconv.Atoi(v); err != nil {
			return opts, errInvalidParam(ParamPage, v)
		}
	}

	if opts.Limit < 0 {
		opts.Limit = 0
	}
	if opts.PageSize < 0 {
		opts.PageSize = 0
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if opts.Page < 0 {
		opts.Page = 0
	}

	// fields can be comma separated and/or repeated.
	var fields []string
	for _, raw := range q[ParamFields] {
		for _, f := range strings.Split(raw, ",") {
			if f = strings.TrimSpace(f); f != "" {
				fields = append(fields, f)
			}
		}
	}
	opts.Fields = fields

	switch strings.ToLower(strings.TrimSpace(q.Get(ParamView))) {
	case ViewFull:
		opts.Full = true
	case ViewSlim:
		opts.Full = false
	}
	if truthy(q.Get(ParamFull)) {
		opts.Full = true
	}

	// Normalize the two pagination conventions into offset/limit. An explicit
	// offset always wins over a derived page offset.
	offsetExplicit := q.Has(ParamOffset)
	if !offsetExplicit && opts.Page > 0 {
		size := opts.PageSize
		if size <= 0 {
			size = opts.Limit
		}
		if size <= 0 {
			size = defaultPageSize
		}
		opts.Limit = size
		opts.Offset = (opts.Page - 1) * size
	}
	if opts.Limit <= 0 && opts.PageSize > 0 {
		opts.Limit = opts.PageSize
	}
	return opts, nil
}

type invalidParamError struct {
	name  string
	value string
}

func (e *invalidParamError) Error() string {
	return "invalid " + e.name + " parameter: " + strconv.Quote(e.value)
}

func errInvalidParam(name, value string) error {
	return &invalidParamError{name: name, value: value}
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// Paginate returns the window selected by offset/limit and the total length of
// the unpaginated slice. limit <= 0 keeps everything after offset. The returned
// slice is never nil for an in-range request, so JSON encodes it as [].
func Paginate[T any](items []T, offset, limit int) ([]T, int) {
	total := len(items)
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []T{}, total
	}
	items = items[offset:]
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	if items == nil {
		items = []T{}
	}
	return items, total
}

// SlimStats is the most recent sample of the metrics a container list view
// usually needs. Keeping a single sample instead of up to 60 historical samples
// is the main payload reduction of the slim view.
type SlimStats struct {
	Timestamp        time.Time `json:"timestamp"`
	CPUUsageTotal    uint64    `json:"cpu_usage_total_ns,omitempty"`
	MemoryUsage      uint64    `json:"memory_usage_bytes,omitempty"`
	MemoryWorkingSet uint64    `json:"memory_working_set_bytes,omitempty"`
}

// SlimContainer is the default projection of a container in list endpoints.
// It carries identity/runtime metadata and one latest resource sample.
type SlimContainer struct {
	Name      string            `json:"name"`
	ID        string            `json:"id,omitempty"`
	Aliases   []string          `json:"aliases,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Image     string            `json:"image,omitempty"`
	Created   time.Time         `json:"created,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Latest    *SlimStats        `json:"latest,omitempty"`
}

// DefaultSlimFields lists the JSON fields retained by the default slim
// projection when it is expressed as a field allow-list over the raw
// ContainerInfo document. SlimContainer itself is the preferred projection;
// this list is useful for tests and documentation of the payload shape.
var DefaultSlimFields = []string{
	"name",
	"id",
	"aliases",
	"namespace",
	"spec.image",
	"spec.creation_time",
	"spec.labels",
}

// NewSlimContainer projects a full ContainerInfo into its slim form.
func NewSlimContainer(c *info.ContainerInfo) SlimContainer {
	if c == nil {
		return SlimContainer{}
	}
	s := SlimContainer{
		Name:      c.Name,
		ID:        c.Id,
		Aliases:   c.Aliases,
		Namespace: c.Namespace,
		Image:     c.Spec.Image,
		Created:   c.Spec.CreationTime,
		Labels:    c.Spec.Labels,
	}
	if n := len(c.Stats); n > 0 && c.Stats[n-1] != nil {
		st := c.Stats[n-1]
		latest := &SlimStats{Timestamp: st.Timestamp}
		if st.Cpu != nil {
			latest.CPUUsageTotal = st.Cpu.Usage.Total
		}
		if st.Memory != nil {
			latest.MemoryUsage = st.Memory.Usage
			latest.MemoryWorkingSet = st.Memory.WorkingSet
		}
		s.Latest = latest
	}
	return s
}

// SlimContainers applies NewSlimContainer to a map of named containers and
// returns the slim values sorted by name.
func SlimContainers(infos map[string]*info.ContainerInfo) []SlimContainer {
	names := make([]string, 0, len(infos))
	for name := range infos {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]SlimContainer, 0, len(names))
	for _, name := range names {
		out = append(out, NewSlimContainer(infos[name]))
	}
	return out
}

// ProjectFields serializes v to generic JSON values and retains only the
// requested fields. Each field is a JSON key or a dotted path into nested
// objects. When v is a slice/array the projection is applied to every element.
// With no fields the (genericized) value is returned unchanged.
func ProjectFields(v any, fields []string) (any, error) {
	if len(fields) == 0 {
		return v, nil
	}
	clean := make([][]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		clean = append(clean, strings.Split(f, "."))
	}
	if len(clean) == 0 {
		return v, nil
	}

	generic, err := toGenericJSON(v)
	if err != nil {
		return nil, err
	}
	return projectGeneric(generic, clean), nil
}

// FilterFields is an alias of ProjectFields using the name some callers expect.
func FilterFields(v any, fields []string) (any, error) {
	return ProjectFields(v, fields)
}

// SelectFields is an alias of ProjectFields.
func SelectFields(v any, fields []string) (any, error) {
	return ProjectFields(v, fields)
}

func toGenericJSON(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func projectGeneric(v any, paths [][]string) any {
	switch node := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for _, p := range paths {
			if val, ok := lookupPath(node, p); ok {
				assignPath(out, p, val)
			}
		}
		return out
	case []any:
		out := make([]any, 0, len(node))
		for _, item := range node {
			out = append(out, projectGeneric(item, paths))
		}
		return out
	default:
		return v
	}
}

// lookupPath walks a dotted path. When an intermediate value is an array the
// remainder of the path is looked up in each element and the collected values
// are returned as []any.
func lookupPath(node any, parts []string) (any, bool) {
	if len(parts) == 0 {
		return node, true
	}
	switch cur := node.(type) {
	case map[string]any:
		next, ok := cur[parts[0]]
		if !ok {
			return nil, false
		}
		return lookupPath(next, parts[1:])
	case []any:
		if len(parts) == 1 {
			return cur, true
		}
		vals := make([]any, 0, len(cur))
		found := false
		for _, item := range cur {
			if v, ok := lookupPath(item, parts); ok {
				vals = append(vals, v)
				found = true
			}
		}
		if !found {
			return nil, false
		}
		return vals, true
	default:
		return nil, false
	}
}

func assignPath(dst map[string]any, parts []string, val any) {
	for i, p := range parts {
		if i == len(parts)-1 {
			dst[p] = val
			return
		}
		next, ok := dst[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			dst[p] = next
		}
		dst = next
	}
}
