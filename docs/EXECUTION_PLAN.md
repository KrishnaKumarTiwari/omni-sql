# OmniSQL: Six-Month Execution Plan

## 1. Team Shape (8.5 FTEs)

| Role | Count | Responsibilities |
|---|---|---|
| **Engineering Manager** | 1 | Delivery, cross-team coordination, stakeholder comms |
| **Backend Engineers** | 3 | Query engine, planner, connector workers, DuckDB integration |
| **Infrastructure/SRE** | 1 | K8s, Terraform, CI/CD, observability, autoscaling |
| **Security Engineer** | 1 | OPA/Rego policies, Vault/KMS, entitlements, threat modeling |
| **QA Engineer** | 1 | Test strategy, integration tests, chaos drills, load testing |
| **Product Manager** | 0.5 | Roadmap prioritization, customer feedback, connector prioritization |
| **Developer Experience** | 0.5 | Connector SDK docs, onboarding playbook, API reference |
| **Eng Lead (you)** | 0.5 | Architecture decisions, code reviews, technical direction |

**Why 8.5 instead of 12**: Leaner team for the first 6 months. The prototype already validates the architecture. We need depth over breadth -- hire a second wave (connector ecosystem team of 3-4) in M4 once the SDK is stable and the connector backlog is clear.

---

## 2. Milestones

### M1: Foundation (Weeks 1-4)

**Goal**: One tenant, two connectors, working end-to-end with basic security.

| Deliverable | Owner | Details |
|---|---|---|
| Connector SDK v0 | Backend | `BaseConnector` interface: connect/fetch/disconnect, auth/token refresh, pagination contract, capability model (tables/fields/ops/limits) |
| GitHub + Jira connectors (real APIs) | Backend | Replace mock data with real HTTP clients. OAuth2 token refresh. Cursor-based pagination. |
| SQL parser (sqlglot) | Backend | Replace string matching with AST parsing. Extract projections, predicates, table refs. Map predicates to connector filter capabilities. |
| Entitlement model skeleton | Security | User token → scopes/roles → RLS/CLS rules. Hardcoded policies first, OPA integration in M3. |
| Rate-limit guardrails | Backend | Token bucket per connector/tenant. `RATE_LIMIT_EXHAUSTED` error with `Retry-After`. |
| K8s namespace isolation | Infra | One namespace per tenant. Network policies restricting cross-tenant traffic. |

**Exit criteria**: Live demo with 1 tenant querying real GitHub + Jira data. `SELECT/WHERE/LIMIT` working. RLS filtering by team.

**Release**: v0.1.0 Internal Alpha

---

### M2: Planner + Freshness (Weeks 5-8)

**Goal**: Smart query planning, caching, and observability baseline.

| Deliverable | Owner | Details |
|---|---|---|
| Query Planner v1 | Backend | Capability discovery per connector. Predicate pushdown decisions (what can the API filter vs. what DuckDB filters). Cost/freshness hints in plan output. |
| Freshness TTL cache | Backend | Redis-backed (replacing in-memory dict). Cache key = `connector + sorted(filters)`. `max_staleness_ms` parameter honored. Conditional requests (ETag/If-Modified-Since) where APIs support it. |
| Per-tenant KMS | Security + Infra | Vault integration for secrets. Tenant-scoped encryption keys. Rotation policy. |
| Observability v1 | Infra | OpenTelemetry traces (full request lifecycle, connector time breakdown). Prometheus metrics (query latency histogram, cache hit rate, rate-limit events). Grafana dashboards. |
| Declarative connector runtime | Backend | `GenericConnector` reads YAML manifests. Onboard Linear as first declarative connector. |

**Exit criteria**: P95 < 1.8s for simple queries. Cache hit ratio > 40% on repeated queries. Traces show connector-level latency breakdown.

**Release**: v0.2.0 Beta

---

### M3: Policy + Async Path (Weeks 9-12)

**Goal**: Production-grade security policies and graceful degradation under load.

| Deliverable | Owner | Details |
|---|---|---|
| Policy DSL (OPA/Rego) | Security | RLS/CLS rules expressed as Rego policies. Compiled into query plans at plan time. Non-engineers can author policies via YAML that compiles to Rego. |
| Async query path | Backend | When rate limits are exhausted or queries are slow, offer async execution with webhook/polling notification. Job queue (Redis/SQS). |
| Error vocabulary + UX | Backend + QA | Standard error codes (`RATE_LIMIT_EXHAUSTED`, `STALE_DATA`, `ENTITLEMENT_DENIED`, `SOURCE_TIMEOUT`). Human-readable messages. `Retry-After` on 429s. Guidance for switching to async. |
| Audit logging | Security | Every cross-system access logged with: user, tenant, query, sources accessed, rows returned (count), timestamp, trace_id. Immutable log (append-only). |
| 3rd + 4th connectors | Backend | Add two more connectors (e.g., Salesforce, Zendesk) to validate SDK generality. |

**Exit criteria**: Clean UX under throttling (rate-limited queries return actionable errors, not 500s). 100% of queries audited. RLS/CLS enforced via Rego.

**Release**: v0.3.0 Secure

---

### M4: Scale + Infrastructure (Weeks 13-16)

**Goal**: Handle production load with automated infrastructure.

| Deliverable | Owner | Details |
|---|---|---|
| Autoscaling | Infra | HPA for Data Plane pods based on QPS + connector queue depth. Cost guardrails (max pod count per tenant). |
| Short-lived materialization | Backend | For joins/aggregations exceeding memory thresholds, spill to DuckDB on disk or Parquet on S3. Lifecycle ≤ 15 minutes. Encrypted per tenant. Auto-cleanup. |
| Helm charts + Terraform modules | Infra | Networking, secrets, databases, clusters as IaC. Environment parity (dev/staging/prod). |
| DR basics | Infra | Multi-AZ deployment. Database backups. RPO < 5 min, RTO < 15 min. |
| Connector worker separation | Backend + Infra | Move connector workers to separate pods (Go or Python with asyncio). Independent scaling from gateway. gRPC between gateway and workers. |

**Exit criteria**: 1k QPS sustained for 10 minutes in synthetic load test. Auto-scale from 2→8 pods and back within 3 minutes. Infrastructure fully reproducible from IaC.

**Release**: v0.4.0 Scale

---

### M5: Multi-Tenant Hardening (Weeks 17-20)

**Goal**: Enterprise-ready isolation, cost controls, and operational maturity.

| Deliverable | Owner | Details |
|---|---|---|
| Tenant isolation hardening | Security + Infra | Storage/compute/network isolation verified. Per-tenant network policies. Optional single-tenant cluster mode (same codebase, different deployment config). |
| Org off-boarding | Security | Automated crypto-shredding: delete tenant KMS key → all encrypted data unrecoverable. Job cancellation. Audit trail of deletion. |
| Cost guardrails | Backend + Infra | Per-tenant API call budgets. Alerts at 80%/90% usage. Admin dashboard showing cost attribution per connector per tenant. |
| Performance tuning | Backend | Connection pooling. Query plan caching. Predicate pushdown coverage analysis (which queries miss pushdown and why). |
| Alerting + runbooks | Infra + QA | Incident playbooks for: rate-limit floods, connector auth failures, cache stampedes, slow connector cascades. PagerDuty integration. |

**Exit criteria**: Performance and cost report delivered. Crypto-shredding demonstrated end-to-end. All Tier-1 alerts have runbooks.

**Release**: v0.5.0 Enterprise

---

### M6: GA Readiness (Weeks 21-24)

**Goal**: Ship it.

| Deliverable | Owner | Details |
|---|---|---|
| GA acceptance criteria | All | All SLOs met for 2 consecutive weeks (P50 < 500ms, P95 < 1.5s, 99.9% uptime). |
| Chaos drills | QA + Infra | Simulate: connector down, cache stampede, rate-limit flood, KMS unavailable, network partition. Verify graceful degradation. |
| Security review | Security | STRIDE threat model documented. Pen-test readiness checklist. TLS everywhere, mTLS between services verified. |
| Canary/blue-green deployment | Infra | Canary: 5% traffic, auto-rollback if P95 > 1.5s or error rate > 0.5%. Blue-green for Control Plane upgrades. |
| Onboarding playbook | DX | New tenant onboarding in < 30 minutes. New connector onboarding (YAML) in < 2 hours. API reference + SDK docs. |
| Data residency | Security + Infra | Data residency tags drive storage and job placement. Connector workers pinned to region matching tenant's residency requirement. |

**Exit criteria**: GA readiness sign-off from EM, Security, and QA. First external tenant onboarded.

**Release**: v1.0.0 GA

---

## 3. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **SaaS API rate limits** vary wildly across providers | High | High | Token bucket per connector with configurable capacity. Async overflow path. Budget allocation across tenants with fairness guarantees. |
| **Connector API drift** (schema changes, deprecations) | Medium | High | Daily automated integration tests against sandbox environments. Connector SDK version pinning. Schema catalog versioning so old queries don't break until explicitly migrated. |
| **Data leakage** across tenants | Low | Critical | K8s namespace isolation + network policies. Per-tenant KMS keys. RLS enforced at plan time (data never enters wrong tenant's scope). Quarterly security review. |
| **Query performance** degrades with complex joins | Medium | Medium | Predicate pushdown reduces data volume. Materialization spill for large joins. Circuit breaker pattern: if connector exceeds 2s timeout, trip breaker and return cached data or `SOURCE_TIMEOUT`. |
| **Quota exhaustion** in multi-tenant | Medium | High | Per-tenant API budgets. Fair queuing (weighted round-robin across tenants). Alerts at 80%/90%. Admin override for burst. |
| **Schema drift** across connector versions | Medium | Medium | Schema catalog with version history. Breaking change detection in CI. Connector manifests pinned to API version. |
| **DuckDB memory pressure** on large joins | Low | Medium | Memory limit per query. Spill to disk/S3 for joins exceeding threshold. Short-lived materialization with TTL cleanup. |
| **Key person dependency** (small team) | Medium | Medium | Pair programming on critical paths. All architectural decisions documented in ADRs. Runbooks for all operational procedures. |

---

## 4. Budget & Infrastructure Assumptions

### Infrastructure (~$12-15k/month)

| Component | Estimate | Notes |
|---|---|---|
| K8s cluster (EKS/GKE) | $4-5k | 3 node groups: system, data-plane, control-plane. Multi-AZ. |
| Redis (ElastiCache) | $1-2k | Cache + distributed rate-limit state + job queue. |
| PostgreSQL (RDS) | $500-1k | Metadata catalog: schemas, policies, tenants, audit logs. |
| Vault (HCP or self-hosted) | $500-1k | Secrets management + per-tenant KMS keys. |
| S3/GCS | $200-500 | Materialization spill, audit log archive, connector manifests. |
| Observability (Datadog/Grafana Cloud) | $2-3k | Traces, metrics, logs, dashboards, alerts. |
| CI/CD (GitHub Actions) | $500 | Build, test, deploy pipelines. |
| Load testing infra | $500 | k6 Cloud or dedicated load-gen instances. |

### Tooling

| Tool | Purpose |
|---|---|
| OPA (Open Policy Agent) | Authorization, RLS/CLS policy engine |
| HashiCorp Vault | Secrets management, tenant-scoped KMS |
| Terraform | Infrastructure as code |
| Helm + ArgoCD | K8s deployment, GitOps |
| OpenTelemetry | Distributed tracing |
| Prometheus + Grafana | Metrics and dashboards |
| k6 | Load testing |
| Snyk | Dependency vulnerability scanning |

### Headcount

| Role | Annual Cost (fully loaded) |
|---|---|
| Senior Engineers (x5) | $200-250k each |
| EM | $250-280k |
| Security Engineer | $220-260k |
| QA Engineer | $180-220k |
| **Total annual** | **~$1.8-2.2M** |

---

## 5. Key Decision Points

| When | Decision | Options | Recommendation |
|---|---|---|---|
| M1 | SQL parser library | sqlglot vs sqlparse vs custom | sqlglot -- best SQL dialect support, active community |
| M2 | Distributed cache | Redis vs Memcached | Redis -- also needed for rate-limit state and job queue |
| M3 | Policy engine | OPA vs Cedar vs custom | OPA -- industry standard, Rego is auditable, large ecosystem |
| M4 | Connector worker language | Python (asyncio) vs Go | Go if connector concurrency becomes the bottleneck; Python otherwise. Measure first. |
| M4 | Materialization backend | DuckDB on disk vs Parquet on S3 | DuckDB on disk for < 1GB joins; Parquet on S3 for larger. Lifecycle ≤ 15 min. |
| M5 | Single-tenant deployment | Separate cluster vs namespace isolation | Namespace isolation by default; dedicated cluster for compliance-heavy customers. Same Helm chart, different values. |
