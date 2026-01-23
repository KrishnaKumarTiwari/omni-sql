# OmniSQL: Six-Month Execution Plan

## 1. Team Shape (10 FTEs)
- **Core Engine (4)**: SQL parsing, query planning, distributed execution, and multi-tenancy logic.
- **Connector Hub (3)**: SDK development, connector versioning, and Tier-1/Tier-2 SaaS integrations.
- **Platform & SRE (2)**: Kubernetes orchestration, BYOC automation, KMS orchestration, and observability (telemetry/tracing).
- **Product & QA (1)**: Requirement validation, SaaS sandbox management, and automated end-to-end testing.

## 2. Milestones
| Timeline | Stage | Goals | Measurable Acceptance Criteria |
| :--- | :--- | :--- | :--- |
| **M 1-2** | Foundational | Stateless engine + core connectors (GH, SF, Slack). | P50 < 500ms; Single-tenant deployment automation ready. |
| **M 3** | Governance | Rate limiting (Token Bucket) + Unified Auth. | Zero-leakage multi-tenancy; 100% audit coverage for queries. |
| **M 4** | Advanced SQL | Cross-app joins + Predicate pushdown optimization. | Support joins across 3+ sources with P95 < 2s. |
| **M 5** | Enterprise | RLS/CLS via OPA + BYOC (Data Plane) launch. | Customer-verified VPC deployment; SOC2/HIPAA compliance signals. |
| **M 6** | Scale | 1000s of connectors (SDK Beta) + Global Edge sharding. | Support 1k QPS aggregate; P99 < 5s for complex federated queries. |

## 3. Risk Register
| Risk | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **SaaS Rate Limits** | High | Aggressive caching + staleness hints + global fairness scheduling. |
| **API Schema Evolution** | Medium | Connector versioning + daily automated "sandbox" smoke tests. |
| **Join Data Volume** | High | Predicate pushdown + broadcast hash joins for small tables. |
| **Data Sovereignty** | Medium | Regional sharding + BYOC Data Plane to keep data resident. |

## 4. Resource & Budget Assumptions
- **Infrastructure**: ~$8k-$12k/mo (AWS/GCP) for staging/prod environments.
- **Tools**: Datadog (monitoring), Snyk (security), OPA (policy engine).
- **Headcount**: High-seniority engineering team (average $180k-$220k OTE).
