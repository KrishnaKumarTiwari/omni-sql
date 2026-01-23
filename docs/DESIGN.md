# OmniSQL: Technical Design Document

## 1. Core Architecture Concepts

OmniSQL is built as a cloud-native, federated query system with a strict separation of concerns.

### 1.1 Control Plane
The "Brain" of the system, responsible for governance and configuration.
- **Tenant & Connector Registry**: Management of customer accounts and available SaaS integrations.
- **Schema Catalog**: Centralized metadata about tables, fields, and operations supported by each connector.
- **Policy Store**: OPA-based definitions for RLS, CLS, and masking rules.
- **Secrets & Rate-Limit Policies**: Secure storage (Vault) and global/tenant-level budget definitions.
- **Audit Logging**: Immutable trail of every query and administrative action.

### 1.2 Data Plane
The "Engine" of the system, optimized for high-throughput, low-latency execution.
- **Query Gateway**: Entry point for all SQL requests.
- **Query Planner & Distributed Executor**: Parses SQL and orchestrates parallel data fetching.
- **Connector Workers**: Specialized sidecars that interface with SaaS APIs.
- **Materialization Layer**: Short-lived storage for complex joins and aggregations.
- **Async Job Runners**: Handles long-running queries that exceed synchronous timeouts.

### 1.3 Join & Execution Strategy
OmniSQL employs a hybrid execution model to balance performance, cost, and complexity:

| Strategy | Mechanism | Best For | Trade-offs |
| :--- | :--- | :--- | :--- |
| **Federated On-the-Fly** | Data is streamed from multiple sources and joined in-memory at the Gateway. | Small datasets, simple filters, low concurrency. | High Gateway memory usage; limited to "small-ish" joins. |
| **Short-Lived Materialization** | Parallel fetch → Spill to high-speed transient storage (DuckDB/S3) → Join → Shred. | Large-scale cross-app joins (e.g., millions of rows). | Transient I/O overhead; higher compute cost. |

#### Technical Rationale:
- **Zero-Persistence Guarantee**: Both strategies ensure data never enters a permanent warehouse. In "Short-Lived Materialization," the transient storage is ephemeral, encrypted with tenant-scoped keys, and wiped immediately post-query.
- **Predicate Pushdown**: We aggressively push filters (WHERE clauses) to the SaaS APIs to minimize data transfer for both strategies.
- **Materialization Trigger**: The Query Planner automatically switches to "Short-Lived Materialization" if the estimated intermediate result set exceeds the Gateway's safe memory threshold (e.g., > 100k rows).

### 1.4 Error Vocabulary
OmniSQL provides a standardized error model for developer clarity:
- `RATE_LIMIT_EXHAUSTED`: Source system or tenant budget has been exceeded.
- `STALE_DATA`: Federated query failed to meet the `max_staleness` constraint.
- `ENTITLEMENT_DENIED`: User lacks the necessary RLS/CLS permissions for the resource.
- `SOURCE_TIMEOUT`: A downstream SaaS provider exceeded the service SLO.
- `PLAN_FAILED`: Query planner could not optimize the federated request.

### 1.5 References
- **Connector Inspiration**: For a comprehensive list of SaaS categories and field mappings, refer to [Merge.dev Categories](https://merge.dev/categories).

---

## 2. Multi-Tenant Isolation & Security

### 2.1 Isolation Strategy
OmniSQL utilizes **Logical Isolation with Physical Guardrails**:
- **Namespace per Tenant**: Every tenant runs in a dedicated K8s namespace/logical boundary.
- **Network Boundaries**: Egress policies ensure no cross-tenant data leakage.
- **Encryption**: Data at rest (caches/materialization) is encrypted with **tenant-scoped keys** managed via Cloud KMS.
- **Sharding**: Compute resources are pooled into "Tiers," with an option for **Single-Tenant Clusters** for high-tier customers.

### 2.2 Security & Compliance
- **TLS & mTLS**: TLS 1.3 enforced everywhere; mTLS for all inter-service communication.
- **AuthN/AuthZ**: AuthN via OIDC; AuthZ via fine-grained OPA policies.
- **Data Residency**: Tags on data and jobs ensure compliance with regional residency requirements (GDPR, etc.).
- **Crypto-Shredding**: Org off-boarding triggers immediate deletion of tenant-scoped KMS keys and job cancellation.
- **Threat Model**: STRIDE analysis is applied to every component, with automated pentest readiness.

### 2.3 Cost Controls
To manage the economics of federated queries at scale, OmniSQL implements multi-layered cost controls:
- **Rate Limit as Cost Proxy**: Global and tenant-level token buckets act as a proxy for egress and operation costs.
- **Query Complexity Quotas**: The Query Optimizer estimates the cost (rows fetched/compute) before execution; expensive joins are rejected with `PLAN_FAILED`.
- **Cache-First Optimization**: High `max_staleness` values are encouraged via pricing or quotas to reduce expensive downstream SaaS calls.
- **Resource Limiting**: Compute for materialization (DuckDB) is capped per-tenant to prevent runaway resource consumption.

---

## 3. Detailed Component Breakdown

| Component | Responsibility | Key Features |
| :--- | :--- | :--- |
| **Query Gateway** | Face of the System | AuthN (OIDC), AuthZ (Policy), request shaping, timeouts. Returns results + metadata (`freshness_ms`, `rate_limit_status`, `trace_id`). |
| **Query Planner** | Intelligence Layer | Capability discovery, predicate/column pushdown, join plan, cost/freshness hints, spill-to-materialization logic. |
| **Connector SDK** | SaaS Interface | Capability model (tables/fields/ops/limits), auth/token refresh, pagination, concurrency contracts, standardized error codes. |
| **Entitlement Svc** | Security Engine | Merges source permissions (e.g., GitHub Teams) with tenant-level policies to compute RLS/CLS at plan time. |
| **Rate-Limit Svc** | Governance | Token buckets/concurrency pools per connector/tenant/user; backoff and budget allocation; async overflow path. |
| **Freshness Layer** | Data Latency | TTL caches, conditional requests (ETag), incremental snapshots; per-source staleness contracts. |
| **Materialization** | Compute Layer | Short-lived tables (DuckDB/Parquet on S3) for massive joins; strictly transient lifecycle (TTL $\le$ N minutes); encrypted per tenant. |
| **Metadata Catalog** | Persistence | Postgres-backed store for schemas, policies, and tenant configurations; migrations via Flyway/Liquibase. |
| **Secrets & Keys** | Security Core | HashiCorp Vault + Cloud KMS (tenant-scoped keys); automated rotation and break-glass protocols. |
| **Observability** | Telemetry | OpenTelemetry traces, Prometheus metrics, structured logging; exemplar-linked dashboards and proactive alerts. |

---

## 4. SQL & Policy Surface

### 4.1 Interface: `POST /v1/query`
The primary interface for both multi-tenant and single-tenant modes.
- **Request Body**:
  ```json
  {
    "sql": "SELECT gh.pr_id FROM github.pull_requests gh JOIN jira.issues jira ON gh.branch = jira.branch_name",
    "metadata": { "trace_id": "abc-123", "max_staleness_ms": 5000 }
  }
  ```
- **Response Body**:
  ```json
  {
    "rows": [...],
    "columns": ["pr_id"],
    "freshness_ms": 124,
    "rate_limit_status": { "remaining": 95, "reset_ms": 60000 },
    "trace_id": "abc-123"
  }
  ```

### 4.2 Policy Configuration
OmniSQL uses a centralized `policy.yaml` (translated to OPA) for unified governance:
```yaml
policies:
  - id: "rls_by_team"
    rule: "SELECT * FROM github.pull_requests WHERE team_id = user.team_id"
  - id: "mask_contributor_email"
    rule: "MASK email WITH 'sha256' FOR ALL EXCEPT role:admin"
```

---

## 5. Capacity & Performance Targets

### 5.1 Sizing & Scaling (1k QPS)
- **Concurrency**: 500 concurrent workers for 1k QPS with 500ms P50 latency.
- **Observability**: Prometheus metrics for `query_latency_seconds` and `connector_errors_total`. Traces covering the full lifecycle (Gateway -> Planner -> Connector).
- **Chaos Plan**: Automated failure injection for connectors; circuit breakers (Hystrix/Resilience4j style) to prevent cascading failures.

---

## 6. Deployment & Operations

- **IaC**: Terraform for VPC, EKS, Vault, RDS.
- **CD**: Helm, Canary/Blue-Green with automatic rollback on SLO (latency/error) regression.
- **Security Protocols**: TLS 1.3 everywhere; mTLS between microservices; per-tenant KMS keys for at-rest encryption.
- **DR/BCP Goals**:
  - **RPO (Recovery Point Objective)**: < 5 minutes for metadata/cache configuration.
  - **RTO (Recovery Time Objective)**: < 15 minutes for Data Plane failover to secondary region.
- **Operational Readiness**: See [OPERATIONS.md](OPERATIONS.md) for detailed Runbooks and deployment strategies.
