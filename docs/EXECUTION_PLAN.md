# OmniSQL: Six-Month Execution Plan

## 1. Team Shape (12 FTEs)
- **Core Engine (4)**: SQL parsing, Query Planning, Distributed Execution, and Materialization (DuckDB).
- **Security & Policy (2)**: OPA integration, Entitlement service, Vault management, and KMS orchestration.
- **Connector Ecosystem (4)**: SDK development, connector versioning, and SaaS API integration.
- **Platform & SRE (2)**: K8s Isolation (Namespaces/Egress), Terraform, and Observability (Otel).

## 2. Milestones
| Timeline | Stage | Goals | Measurable Acceptance Criteria |
| :--- | :--- | :--- | :--- |
| **M 1-2** | Foundational | Multi-tenant Data Plane + Core Connectors. | K8s namespace isolation; P50 < 500ms for pushdown. |

### 2.1 Granular Sprints (Months 1-2)

**Sprint 1 (Wk 1-2): Core Engine Skeleton**
- [x] Initial Query Gateway (FastAPI) setup
- [x] DuckDB materialization integration
- [ ] Basic SQL parser with filter extraction
- [ ] **Release**: v0.1.0 Internal Alpha

**Sprint 2 (Wk 3-4): Connector Protocol & SDK**
- [ ] Define standardized Connect/Fetch/Disconnect interface
- [ ] Implement GitHub & Jira Connectors (v1)
- [ ] Add rate limiter sidecar pattern
- [ ] **Release**: v0.2.0 Connector SDK

**Sprint 3 (Wk 5-6): Security Layer MVP**
- [ ] OPA integration for Policy Decisions
- [ ] Implement RLS (Team-based) filtering
- [ ] Implement CLS (Masking) logic
- [ ] **Release**: v0.3.0 Secure Engine

**Sprint 4 (Wk 7-8): Caching & Performance**
- [ ] Implement TTL-based Metadata Cache
- [ ] Add "Tidal" caching strategy for different tiers
- [ ] Load testing & benchmarks baseline
- [ ] **Release**: v0.4.0 Foundation Complete
| **M 3** | Governance | OPA Entitlements + Rate Limiting. | RLS/CLS enforced via Rego; 100% audit coverage. |
| **M 4** | Advanced SQL | Cross-app joins + Materialization (DuckDB). | Support complex joins with spilled materialization. |
| **M 5** | Enterprise | BYOC Data Plane + Vault/KMS (Tenant Keys). | Proof of Org off-boarding (crypto-shredding) verified. |
| **M 6** | Scale | Global Edge Sharding + 1k QPS aggregate. | P95 < 1.5s for single-source; P99 < 5s for joins. |

## 3. Risk Register
| Risk | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **SaaS Rate Limits** | High | Token bucket fairness + async query overflow mode. |
| **Data Leakage** | Critical | Strict K8s egress policies + Tenant-scoped KMS keys. |
| **SQL Performance** | Medium | Predicate pushdown + materialization lifecycle management. |
| **API Drift** | Medium | Daily automated sandbox integration tests. |

## 4. Resource & Budget Assumptions
- **Infrastructure**: ~$12k-$15k/mo (AWS/GCP) for multi-region staging/prod.
- **Tools**: Datadog, Snyk, HashiCorp Vault, Open Policy Agent (OPA).
- **Headcount**: Senior engineering team (Avg $200k OTE).
- **Operations**: 24/7 on-call rotation for Tier-1 connectors.
