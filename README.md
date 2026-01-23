# OmniSQL: Universal SQL Across Enterprise Apps

OmniSQL is a high-performance federated query layer designed to query multiple enterprise SaaS applications at scale. It allows developers and data teams to treat their entire SaaS stack as a single, unified SQL database.

## 🚀 Quicklinks
- **High-Level Design**: [docs/DESIGN.md](docs/DESIGN.md)
- **Six-Month Execution Plan**: [docs/EXECUTION_PLAN.md](docs/EXECUTION_PLAN.md)
- **Prototype Documentation**: [prototype/PROTOTYPE.md](prototype/PROTOTYPE.md)

---

## 🎯 Architecture Rationale & Trade-offs

### 1. Federated On-Demand Execution
**Decision**: Real-time Query Layer over ETL/Warehousing.
- **Rationale**: Operational workflows (e.g., customer support, real-time risk assessment) cannot wait for hourly ETL syncs. OmniSQL prioritizes **freshness** over historical analysis depth.
- **Trade-off**: Higher complexity in managing heterogeneous SaaS rate limits and varied API response times.

### 2. Stateless Data Plane with "Just-In-Time" Caching
**Decision**: No persistent storage of source data.
- **Rationale**: Minimizes compliance surface (GDPR/CCPA/SOC2). Statelessness allows for effortless horizontal scaling and regional sharding to meet data residency requirements.
- **Trade-off**: Slightly higher P99 latencies for complex, un-cached cross-app joins.

### 3. Aggressive Predicate Pushdown
**Decision**: Push filtering/projection to the source API.
- **Rationale**: Minimizing data ingress is the primary way to achieve sub-second P50 latencies. We fetch only what is necessary.
- **Trade-off**: SQL richness is sometimes bounded by the capabilities of the underlying SaaS (e.g., non-indexed filters).

### 4. Hybrid Control/Data Plane (BYOC Support)
**Decision**: Decouple the query execution engine (Data Plane) from the management logic (Control Plane).
- **Rationale**: Large enterprises often require data to remain within their network. BYOC (Bring Your Own Cloud) is a first-class citizen.
- **Trade-off**: Significant engineering overhead in maintaining cross-cloud deployment automation.

---

## 📈 Targets & SLOs
- **Scale**: 10M Users | 1k QPS | 1000s of Connectors
- **Throughput**: 100 MB/s aggregate bandwidth
- **Latency**: P50 < 500ms | P95 < 1.5s
- **Availability**: 99.9% Monthly Uptime

---

## 🛠 Prototype Scenario
The current prototype demonstrates a cross-app join between **GitHub** and **Jira** to track the status of Pull Requests against their corresponding Jira Issues.

**Sample Query**:
```sql
SELECT gh.pr_id, gh.status, jira.issue_key, jira.status as jira_status
FROM github.pull_requests gh
JOIN jira.issues jira ON gh.branch = jira.branch_name
WHERE jira.status = 'In Progress'
```

---

## 🎬 Quick Demo

### Interactive Web Console

Access the web console at `http://localhost:8000/` for one-click query demos with a beautiful Ema.co-themed interface.

### Cross-App Query Examples

#### 1. Issues to PR Mapping (Core Federated Join)
```bash
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -H "X-User-Token: token_dev" \
  -d '{
    "sql": "SELECT gh.pr_id, gh.author, gh.branch, ji.issue_key, ji.status as jira_status FROM github.pull_requests gh JOIN jira.issues ji ON gh.branch = ji.branch_name WHERE ji.status = '\''In Progress'\''",
    "metadata": {"max_staleness_ms": 0}
  }'
```

**What this demonstrates**:
- Real-time join across GitHub (120 PRs) and Jira (120 issues)
- RLS filtering (only shows data for user's team)
- CLS masking (email addresses hashed for non-PII users)
- Freshness tracking (live fetch vs cache)

#### 2. Security Audit Query
```bash
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -H "X-User-Token: token_dev" \
  -d '{
    "sql": "SELECT gh.pr_id, gh.merged_at, ji.issue_key, ji.status FROM github.pull_requests gh JOIN jira.issues ji ON gh.branch = ji.branch_name WHERE gh.status = '\''merged'\'' AND ji.status != '\''Done'\''",
    "metadata": {"max_staleness_ms": 5000}
  }'
```

**What this demonstrates**:
- Finding merged PRs with unresolved Jira tickets
- Cache usage (5s staleness tolerance)
- Rate limit tracking in response

#### 3. Effort Mapping (Story Points vs Code Changes)
```bash
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -H "X-User-Token: token_dev" \
  -d '{
    "sql": "SELECT ji.issue_key, ji.story_points, (gh.additions + gh.deletions) as total_loc FROM jira.issues ji JOIN github.pull_requests gh ON ji.branch_name = gh.branch ORDER BY total_loc DESC LIMIT 10",
    "metadata": {}
  }'
```

---

## 🏗 Architecture
