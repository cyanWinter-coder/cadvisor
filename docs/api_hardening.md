# cAdvisor HTTP API hardening

This document describes the request-layer protections added for deployments
managing tens of thousands of containers, where an unbounded list request can
take tens of seconds and front-end retries can saturate the CPU.

## Paginated, field-filtered container list

A hardened list endpoint is available under every v2 API version that embeds
v2.0:

```
GET /api/v2.0/containers
```

The default response is a **slim** projection (identity, image, creation time,
labels and the single latest CPU/memory sample), wrapped in a stable page
envelope:

```json
{
  "items": [
    {
      "name": "/docker/...",
      "id": "...",
      "aliases": ["..."],
      "namespace": "docker",
      "image": "nginx:latest",
      "created": "2026-...Z",
      "latest": {
        "timestamp": "2026-...Z",
        "cpu_usage_total_ns": 123,
        "memory_usage_bytes": 4096,
        "memory_working_set_bytes": 2048
      }
    }
  ],
  "total": 12345,
  "limit": 100,
  "offset": 200
}
```

Query parameters:

| Parameter              | Meaning                                                        |
|------------------------|----------------------------------------------------------------|
| `limit=N`              | Maximum number of items (alias `page_size`/`per_page`).        |
| `offset=N`             | Number of items to skip.                                       |
| `page=P`               | 1-based page; converted with `page_size` (default 100).         |
| `page_size=N`          | Page size used with `page`.                                    |
| `fields=a,b,spec.image`| Comma-separated JSON fields / dotted paths to retain.          |
| `full=true`/`view=full`| Return the complete `ContainerInfo` instead of the slim view.  |

Malformed paging parameters produce `400 Bad Request` with a JSON `error`.

All pre-existing routes and response shapes are unchanged; the historical
`/api/v1.*` and `/api/v2.0|v2.1` endpoints keep working as before.

## Per-request timeout

Every non-exempt request is bounded by `-request_timeout` (default `30s`). If
the deadline expires before the handler starts its response, the server returns
an explicit, retryable status instead of leaving the client hanging:

```
HTTP/1.1 504 Gateway Timeout
Content-Type: application/json

{"error":"request timed out"}
```

Long-lived event streams (`stream=true`) and `/debug/pprof/*` are exempt.

## Independent health check

`/healthz` is manager-free (always `200 ok`) and is mounted **outside** the
timeout, request-coalescing and concurrency gates. Liveness probes therefore
succeed even when every business-query slot is occupied. It is still counted
by path.

## Identical-query merging

Identical safe `GET /api/` queries are merged in two ways:

* concurrent identical requests execute the underlying query once
  (single-flight) and share the result;
* successful JSON responses are reused for a short window controlled by
  `-api_coalesce_ttl` (default `3s`), so an immediate front-end retry does not
  trigger a second full container fan-out.

Event streams are never merged.

## Per-path request counts and slow-request logs

Requests are counted by normalized route label (`/api/v2.0/stats`, not
`/api/v2.0/stats/docker/<container-id>`) to keep cardinality bounded. Requests
taking at least `-slow_request_threshold` (default `1s`) are logged with their
method, path, status and duration.

A JSON snapshot of per-path counters and recent slow requests is served at
`-http_request_metrics_endpoint` (default `/debug/http-requests`; set empty to
disable).

The number of concurrently executing heavy requests is bounded by
`-api_max_concurrent_requests` (default `64`; `0` disables the bound). Requests
that wait past their deadline are completed with the timeout status.
