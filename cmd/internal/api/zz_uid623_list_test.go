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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	info "github.com/google/cadvisor/info/v1"
	v2 "github.com/google/cadvisor/info/v2"
	"github.com/google/cadvisor/lib/manager"
	model "github.com/google/cadvisor/lib/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func uid623ReqURL(t *testing.T, raw string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, raw, nil)
	require.NoError(t, err)
	return r
}

func TestUID623_ParseListOptionsLimitOffset(t *testing.T) {
	opts := ParseListOptions(uid623ReqURL(t, "http://x/api/v2.0/containers?limit=25&offset=50"))
	assert.Equal(t, 25, opts.Limit)
	assert.Equal(t, 50, opts.Offset)
}

func TestUID623_ParseListOptionsPage(t *testing.T) {
	opts := ParseListOptions(uid623ReqURL(t, "http://x/api/v2.0/containers?page=3&page_size=10"))
	assert.Equal(t, 3, opts.Page)
	assert.Equal(t, 10, opts.PageSize)
	assert.Equal(t, 10, opts.Limit)
	assert.Equal(t, 20, opts.Offset)
}

func TestUID623_ParseListOptionsPageDefaultSize(t *testing.T) {
	opts := ParseListOptions(uid623ReqURL(t, "http://x/api/v2.0/containers?page=2"))
	assert.Equal(t, defaultPageSize, opts.Limit)
	assert.Equal(t, defaultPageSize, opts.Offset)
}

func TestUID623_ParseListOptionsOffsetWinsOverPage(t *testing.T) {
	opts := ParseListOptions(uid623ReqURL(t, "http://x?page=4&page_size=5&offset=2"))
	assert.Equal(t, 2, opts.Offset)
}

func TestUID623_ParseListOptionsFields(t *testing.T) {
	opts := ParseListOptions(uid623ReqURL(t, "http://x?fields=name%2Cid&fields=spec.image"))
	assert.Equal(t, []string{"name", "id", "spec.image"}, opts.Fields)
}

func TestUID623_ParseListOptionsFull(t *testing.T) {
	assert.True(t, ParseListOptions(uid623ReqURL(t, "http://x?full=true")).Full)
	assert.True(t, ParseListOptions(uid623ReqURL(t, "http://x?full=1")).Full)
	assert.True(t, ParseListOptions(uid623ReqURL(t, "http://x?view=full")).Full)
	assert.False(t, ParseListOptions(uid623ReqURL(t, "http://x?view=slim&full=false")).Full)
}

func TestUID623_ParseListOptionsInvalid(t *testing.T) {
	_, err := ParseListOptionsChecked(uid623ReqURL(t, "http://x?limit=abc"))
	assert.Error(t, err)
	_, err = ParseListOptionsChecked(uid623ReqURL(t, "http://x?page=notanint"))
	assert.Error(t, err)
}

func TestUID623_Paginate(t *testing.T) {
	items := []int{0, 1, 2, 3, 4}

	page, total := Paginate(items, 0, 2)
	assert.Equal(t, 5, total)
	assert.Equal(t, []int{0, 1}, page)

	page, _ = Paginate(items, 2, 2)
	assert.Equal(t, []int{2, 3}, page)

	page, _ = Paginate(items, 4, 10)
	assert.Equal(t, []int{4}, page)

	page, _ = Paginate(items, 9, 2)
	assert.NotNil(t, page)
	assert.Empty(t, page)

	page, _ = Paginate(items, 0, 0)
	assert.Len(t, page, 5)
}

func TestUID623_ProjectFieldsTopLevelAndDotted(t *testing.T) {
	v := map[string]any{
		"name":      "c1",
		"id":        "abc",
		"secret":    "hide",
		"namespace": "docker",
		"spec": map[string]any{
			"image":   "img:latest",
			"cpu":     map[string]any{"x": 1},
			"labels":  map[string]any{"a": "b"},
			"ignored": true,
		},
	}
	out, err := ProjectFields(v, []string{"name", "spec.image"})
	require.NoError(t, err)
	b, err := json.Marshal(out)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, "c1", got["name"])
	_, hasID := got["id"]
	assert.False(t, hasID)
	_, hasSecret := got["secret"]
	assert.False(t, hasSecret)
	spec, ok := got["spec"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "img:latest", spec["image"])
	_, hasCPU := spec["cpu"]
	assert.False(t, hasCPU)
}

func TestUID623_ProjectFieldsAppliesToSlices(t *testing.T) {
	in := []map[string]any{{"name": "a", "drop": 1}, {"name": "b", "drop": 2}}
	out, err := ProjectFields(in, []string{"name"})
	require.NoError(t, err)
	b, _ := json.Marshal(out)
	var got []map[string]any
	require.NoError(t, json.Unmarshal(b, &got))
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0]["name"])
	_, hasDrop := got[0]["drop"]
	assert.False(t, hasDrop)
}

func TestUID623_SlimContainerUsesLatestSample(t *testing.T) {
	ts := time.Now().Add(-time.Second)
	ci := &info.ContainerInfo{
		ContainerReference: info.ContainerReference{Name: "/docker/c1", Id: "id1", Namespace: "docker", Aliases: []string{"c1"}},
		Spec: info.ContainerSpec{
			Image:        "nginx:1",
			CreationTime: ts,
			Labels:       map[string]string{"app": "web"},
		},
		Stats: []*info.ContainerStats{
			{Timestamp: ts.Add(-time.Minute), Cpu: &model.CpuStats{Usage: model.CpuUsage{Total: 1}}},
			{Timestamp: ts, Cpu: &model.CpuStats{Usage: model.CpuUsage{Total: 12345}}, Memory: &model.MemoryStats{Usage: 4096, WorkingSet: 2048}},
		},
	}
	slim := NewSlimContainer(ci)
	assert.Equal(t, "/docker/c1", slim.Name)
	assert.Equal(t, "id1", slim.ID)
	assert.Equal(t, "nginx:1", slim.Image)
	require.NotNil(t, slim.Latest)
	assert.Equal(t, uint64(12345), slim.Latest.CPUUsageTotal)
	assert.Equal(t, uint64(4096), slim.Latest.MemoryUsage)
	assert.Equal(t, uint64(2048), slim.Latest.MemoryWorkingSet)
}

// uid623FakeManager implements just the methods the list handler needs while
// satisfying manager.Manager through interface embedding.
type uid623FakeManager struct {
	manager.Manager
	infos        map[string]*info.ContainerInfo
	err          error
	gotName      string
	gotCount     int
	gotRecursive bool
}

func (f *uid623FakeManager) GetRequestedContainersInfo(name string, opts v2.RequestOptions) (map[string]*info.ContainerInfo, error) {
	f.gotName = name
	f.gotCount = opts.Count
	f.gotRecursive = opts.Recursive
	return f.infos, f.err
}

func uid623SampleInfos(n int) map[string]*info.ContainerInfo {
	out := map[string]*info.ContainerInfo{}
	for i := 0; i < n; i++ {
		name := "/docker/c" + string(rune('a'+i))
		out[name] = &info.ContainerInfo{
			ContainerReference: info.ContainerReference{Name: name, Id: "id-" + string(rune('a'+i))},
			Spec:               info.ContainerSpec{Image: "img-" + string(rune('a'+i))},
		}
	}
	return out
}

func TestUID623_HandleContainersListDefaultSlimEnvelope(t *testing.T) {
	fm := &uid623FakeManager{infos: uid623SampleInfos(3)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers")

	require.NoError(t, handleContainersList(nil, fm, rec, r))
	assert.Equal(t, http.StatusOK, rec.Code)
	// slim view requests only the latest sample
	assert.Equal(t, 1, fm.gotCount)
	assert.True(t, fm.gotRecursive)

	var env ListResponse[json.RawMessage]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, 3, env.Total)
	require.Len(t, env.Items, 3)

	var first SlimContainer
	require.NoError(t, json.Unmarshal(env.Items[0], &first))
	assert.Equal(t, "/docker/ca", first.Name)
	assert.Equal(t, "img-a", first.Image)
}

func TestUID623_HandleContainersListPagination(t *testing.T) {
	fm := &uid623FakeManager{infos: uid623SampleInfos(5)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers?limit=2&offset=1")

	require.NoError(t, handleContainersList(nil, fm, rec, r))
	var env ListResponse[json.RawMessage]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, 5, env.Total)
	assert.Equal(t, 2, env.Limit)
	assert.Equal(t, 1, env.Offset)
	require.Len(t, env.Items, 2)

	var a, b SlimContainer
	require.NoError(t, json.Unmarshal(env.Items[0], &a))
	require.NoError(t, json.Unmarshal(env.Items[1], &b))
	assert.Equal(t, "/docker/cb", a.Name)
	assert.Equal(t, "/docker/cc", b.Name)
}

func TestUID623_HandleContainersListPageEnvelope(t *testing.T) {
	fm := &uid623FakeManager{infos: uid623SampleInfos(5)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers?page=2&page_size=2")

	require.NoError(t, handleContainersList(nil, fm, rec, r))
	var env ListResponse[json.RawMessage]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, 5, env.Total)
	assert.Equal(t, 2, env.Page)
	assert.Equal(t, 3, env.Pages)
	require.Len(t, env.Items, 2)
}

func TestUID623_HandleContainersListFields(t *testing.T) {
	fm := &uid623FakeManager{infos: uid623SampleInfos(1)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers?fields=name,id")

	require.NoError(t, handleContainersList(nil, fm, rec, r))
	var env struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	require.Len(t, env.Items, 1)
	assert.Contains(t, env.Items[0], "name")
	assert.Contains(t, env.Items[0], "id")
	assert.NotContains(t, env.Items[0], "spec")
}

func TestUID623_HandleContainersListFull(t *testing.T) {
	fm := &uid623FakeManager{infos: uid623SampleInfos(1)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers?full=true&count=10")

	require.NoError(t, handleContainersList(nil, fm, rec, r))
	assert.Equal(t, 10, fm.gotCount)
	var env struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	require.Contains(t, env.Items[0], "spec")
}

func TestUID623_HandleContainersListBadLimit(t *testing.T) {
	fm := &uid623FakeManager{infos: uid623SampleInfos(1)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers?limit=nope")

	require.NoError(t, handleContainersList(nil, fm, rec, r))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "error")
}

func TestUID623_HandleRequestRoutesContainersList(t *testing.T) {
	versions := getAPIVersions()
	supported := map[string]ApiVersion{}
	for _, v := range versions {
		supported[v.Version()] = v
	}
	fm := &uid623FakeManager{infos: uid623SampleInfos(2)}
	rec := httptest.NewRecorder()
	r := uid623ReqURL(t, "http://x/api/v2.0/containers?limit=1")

	require.NoError(t, handleRequest(supported, fm, rec, r))
	assert.Equal(t, http.StatusOK, rec.Code)
	var env ListResponse[json.RawMessage]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, 2, env.Total)
	require.Len(t, env.Items, 1)
}

func TestUID623_WriteContainerInfoListDefaultBareArray(t *testing.T) {
	all := []*info.ContainerInfo{
		{ContainerReference: info.ContainerReference{Name: "a", Id: "id-a"}},
		{ContainerReference: info.ContainerReference{Name: "b", Id: "id-b"}},
		{ContainerReference: info.ContainerReference{Name: "c", Id: "id-c"}},
	}
	rec := httptest.NewRecorder()
	require.NoError(t, writeContainerInfoList(rec, all, uid623ReqURL(t, "http://x/api/v1.3/subcontainers")))

	var got []map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 3, "default response must remain the full bare array")
	assert.Contains(t, got[0], "id")
}

func TestUID623_WriteContainerInfoListPaginatesAndFilters(t *testing.T) {
	all := []*info.ContainerInfo{
		{ContainerReference: info.ContainerReference{Name: "a", Id: "id-a"}},
		{ContainerReference: info.ContainerReference{Name: "b", Id: "id-b"}},
		{ContainerReference: info.ContainerReference{Name: "c", Id: "id-c"}},
	}

	rec := httptest.NewRecorder()
	require.NoError(t, writeContainerInfoList(rec, all, uid623ReqURL(t, "http://x?limit=2&offset=1")))
	var page []map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Len(t, page, 2)
	assert.Equal(t, "b", page[0]["name"])
	assert.Equal(t, "c", page[1]["name"])

	recFields := httptest.NewRecorder()
	require.NoError(t, writeContainerInfoList(recFields, all, uid623ReqURL(t, "http://x?limit=1&fields=name")))
	var filtered []map[string]any
	require.NoError(t, json.Unmarshal(recFields.Body.Bytes(), &filtered))
	require.Len(t, filtered, 1)
	assert.Contains(t, filtered[0], "name")
	assert.NotContains(t, filtered[0], "id")
}
