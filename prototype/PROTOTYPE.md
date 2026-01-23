# OmniSQL Prototype: Comprehensive Federated Query

This prototype demonstrates a cross-app join between **GitHub** and **Jira**, incorporating the core architecture concepts of OmniSQL.

## 🏗 Prototype Mapping to Modular Design

The prototype implements the core logic of the modular architecture within a simplified codebase:

1. **Query Gateway**: [prototype/main.py](main.py)
   - Handles `POST /v1/query`, AuthN simulation, and metadata response shaping.
   
2. **Query Planner & Materialization**: [prototype/engine.py](engine.py)
   - DuckDB acts as the transient materialization layer.
   
3. **Connector SDK**: [prototype/connectors/base.py](connectors/base.py)
   - Standardized interface for SaaS interactions.
   
4. **Entitlement Service**: [prototype/utils/security.py](utils/security.py)
   - Implements OPA-style RLS and CLS (Masking).
   
5. **Rate-Limit Service**: [prototype/governance/rate_limit.py](governance/rate_limit.py)
   - Thread-safe Token Bucket implementation.
   
6. **Observability**: Integrated via `prometheus_client` in `main.py`.

---

## 🏗 Join & Execution Strategy: Short-Lived Materialization

This prototype demonstrates OmniSQL's **Short-Lived Materialization** strategy, which is the "gold standard" for complex, high-volume cross-app joins:

1. **Parallel Ingress**: Data is fetched from GitHub and Jira simultaneously.
2. **Predicate Pushdown**: Filters (e.g., `jira.status = 'In Progress'`) are pushed to the mock connectors to minimize data ingress into the prototype.
3. **Transient Materialization**: The filtered datasets are "shredded" into an in-memory **DuckDB** instance. DuckDB acting as a high-speed, transient execution engine.
4. **Final Assembly**: The complex join is performed within DuckDB.
5. **Auto-Shredding**: The in-memory data is automatically wiped when the request lifecycle ends, ensuring zero persistence.

---

## 4. SQL & Policy Surface Implementation

The prototype demonstrates the core concepts of the SQL and Policy surface:

### 4.1 SQL Subset Execution
The `FederatedEngine` ([engine.py](prototype/engine.py)) simulates a query planner that:
1.  Fetches raw data from connectors.
2.  Applies security policies.
3.  Registers data as temporary views in **DuckDB**.
4.  Executes a join query (simulating the compiled plan).

### 4.2 Policy DSL Prototype
The policies are currently implemented in [security.py](prototype/utils/security.py) as Python logic, mimicking the behavior of a compiled DSL:

- **RLS**: The `apply_rls` method filters list items based on the `user_context["team_id"]`.
- **CLS**: The `apply_cls` method uses `hashlib` to mask `author_email` and hides columns for specific roles (e.g., hiding `author` for `qa` role).

### 4.3 Compilation Simulation
In a production system, this would be a code-generation or AST-transformation step. In the prototype, this is seen in `engine.py` where:
- Security methods are called *immediately after* data is fetched from the "source" (simulating predicate pushdown/early filter).
- The final SQL query in DuckDB operates only on this "safe" filtered data.

---

## 5. 🚀 How to Run & Verify

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

## 6. 🧪 Verification Criteria
1. **Security**: Role `qa` cannot see `gh.author` (CLS masking).
2. **Entitlements**: `Team: Mobile` only sees PRs with `team_id = 'mobile'`.
3. **Rate Limits**: 429 response after 50 rapid requests, with `Retry-After` header.
4. **Performance**: Log shows `trace_id` and connector fetch time (aiming for P50 < 500ms).
5. **Freshness**: Response field `freshness_ms` indicates if data was served from cache (< 100ms) or live.
