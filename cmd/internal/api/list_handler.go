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
	"net/http"
	"sort"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/lib/manager"
)

// containersListPage serves the hardened container list endpoint
// (/api/<version>/containers).
//
// Unlike the historical stats/subcontainers endpoints it:
//   - returns a slim projection (identity + image/labels + latest sample) by
//     default instead of up to 60 full samples per container;
//   - supports limit/offset and page/page_size pagination;
//   - supports arbitrary fields projection (?fields=name,spec.image,...) over
//     the complete document;
//   - wraps the page in a stable {items,total,limit,offset,...} envelope.
//
// Pass full=true (or view=full) for complete ContainerInfo objects.
func handleContainersList(request []string, m manager.Manager, w http.ResponseWriter, r *http.Request) error {
	opts, err := ParseListOptionsChecked(r)
	if err != nil {
		return writeBadRequest(w, err)
	}

	reqOpts, err := GetRequestOptions(r)
	if err != nil {
		return writeBadRequest(w, err)
	}
	// The slim view only needs the most recent sample.
	if !opts.Full && len(opts.Fields) == 0 {
		reqOpts.Count = 1
	}
	reqOpts.Recursive = true

	name := getContainerName(request)
	infos, err := m.GetRequestedContainersInfo(name, reqOpts)
	if err != nil && len(infos) == 0 {
		return err
	}

	names := make([]string, 0, len(infos))
	for n := range infos {
		names = append(names, n)
	}
	sort.Strings(names)

	pagedNames, total := Paginate(names, opts.Offset, opts.Limit)

	items := make([]any, 0, len(pagedNames))
	for _, n := range pagedNames {
		ci := infos[n]
		switch {
		case len(opts.Fields) > 0:
			// Explicit fields are resolved against the full document so any
			// JSON field of ContainerInfo is selectable.
			projected, perr := ProjectFields(ci, opts.Fields)
			if perr != nil {
				return perr
			}
			items = append(items, projected)
		case opts.Full:
			items = append(items, ci)
		default:
			items = append(items, NewSlimContainer(ci))
		}
	}

	resp := ListResponse[any]{
		Items:  items,
		Total:  total,
		Offset: opts.Offset,
	}
	if opts.Limit > 0 {
		resp.Limit = opts.Limit
	}
	if opts.Page > 0 {
		resp.Page = opts.Page
		if opts.Limit > 0 {
			pages := total / opts.Limit
			if total%opts.Limit != 0 {
				pages++
			}
			resp.Pages = pages
		}
	}
	return writeResult(resp, w)
}

// writeBadRequest writes a JSON 400 response and returns nil so the caller
// does not write a second (500) response.
func writeBadRequest(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = writeJSONError(w, err.Error())
	return nil
}

func writeJSONError(w http.ResponseWriter, msg string) error {
	return writeResult(map[string]string{"error": msg}, w)
}

// writeContainerInfoList renders a legacy []*ContainerInfo list (the
// /api/v1.x/subcontainers response). Its default shape is unchanged; opt-in
// limit/offset pagination slices the array and fields projects each element.
func writeContainerInfoList(w http.ResponseWriter, containers []*info.ContainerInfo, r *http.Request) error {
	opts := ParseListOptions(r)
	if opts.Offset > 0 || opts.Limit > 0 {
		containers, _ = Paginate(containers, opts.Offset, opts.Limit)
	}
	if len(opts.Fields) == 0 {
		return writeResult(containers, w)
	}
	items := make([]any, 0, len(containers))
	for _, c := range containers {
		projected, err := ProjectFields(c, opts.Fields)
		if err != nil {
			return err
		}
		items = append(items, projected)
	}
	return writeResult(items, w)
}
