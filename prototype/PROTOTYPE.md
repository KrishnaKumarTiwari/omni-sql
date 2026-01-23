# OmniSQL Prototype: Comprehensive Federated Query

This prototype demonstrates a cross-app join between **GitHub** and **Jira**, incorporating the core architecture concepts of OmniSQL.

## 🏗 Prototype Components

1. **Query Gateway**:
   - **Interface**: `POST /v1/query` (Accepts SQL, returns JSON results + metadata).
   - **AuthN/AuthZ**: Simulates OIDC token validation and OPA entitlement checks.

2. **Policy Configuration**:
   - **RLS**: Filters PRs based on the user's `team_id`.
   - **CLS**: Masks the `author_email` field for users without the `PII_ACCESS` scope.

3. **Governance**:
   - **Rate Limiting**: Token Bucket with burst support (50 tokens, 10/sec refill).
   - **Freshness**: `max_staleness_ms` parameter to toggle between cache and live fetch.

4. **Observability**:
   - **Prometheus**: Tracks `http_requests_total` and `query_latency_ms`.
   - **Tracing**: Traces the full request path including connector call time.

---

## 🏗 Join & Execution Strategy

This prototype demonstrates a **Short-Lived Materialization** strategy:
- Data is fetched from GitHub and Jira concurrently.
- Filters are pushed down to the mock connectors (Predicate Pushdown).
- The resulting filtered datasets are "materialized" into an in-memory **DuckDB** instance.
- The complex cross-app join is performed within DuckDB, simulating a transient execution environment that is wiped after the query lifecycle.

---

## 📖 Error Vocabulary

The following standardized errors are implemented in this prototype:
- `RATE_LIMIT_EXHAUSTED`: Returned when the mock connector's token bucket is empty.
- `STALE_DATA`: (Simulated via logs) when cache constraints are not met.
- `ENTITLEMENT_DENIED`: Returned as a 403 when OPA-style RLS/CLS rules block access.
- `SOURCE_TIMEOUT`: (Simulated) when downstream network latency exceeds the gateway timeout.

---

## 🚀 How to Run & Verify

### 1. Prerequisites
- Python 3.9+
- `pip install uvicorn fastapi duckdb prometheus_client opentelemetry-api`
- `k6` (for load testing)

### 2. Execution
```bash
# Start the Prototype API
python prototype/main.py

# Run a sample query
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT gh.pr_id, jira.status FROM github.pull_requests gh JOIN jira.issues jira ON gh.branch = jira.branch_name WHERE jira.status = \"In Progress\""}'
```

### 3. Load Testing (k6)
```bash
# Target: 500-1k QPS for 60s
k6 run prototype/tests/load_test.js
```

---

## 🧪 Verification Criteria
1. **Security**: Role `qa` cannot see `gh.diff_link` (CLS masking).
2. **Entitlements**: `Team: Mobile` only sees PRs with `team_id = 'mobile'`.
3. **Rate Limits**: 429 response after 50 rapid requests, with `Retry-After` header.
4. **Performance**: Log shows `trace_id` and connector fetch time (aiming for P50 < 500ms).
5. **Freshness**: Response field `freshness_ms` indicates if data was served from cache (< 100ms) or live.
