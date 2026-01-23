# OmniSQL: Six-Month Execution Plan

## 1. Team Shape (10 FTEs)
- **Core Engine (4)**: Query planner, distributed executor, multi-tenancy.
- **Connector Team (4)**: SDK development and SaaS integration.
- **Platform/SRE (2)**: Infrastructure, BYOC automation, and observability.

## 2. Milestones
| Timeline | Stage | Goals | Acceptance Criteria |
| :--- | :--- | :--- | :--- |
| **Months 1-2** | Foundational | Stateless engine + 3 Tier-0 connectors. | P50 < 500ms for single-source queries. |
| **Months 3-4** | Scale & Governance | Rate limiting, RLS/CLS, and Result Caching. | Support 100 concurrent tenants without noise. |
| **Months 5-6** | Enterprise Ready | Launch BYOC mode & 100+ Tier-1 connectors. | Successful POC in customer VPC. |

## 3. Risk Register
| Risk | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **SaaS API Drift** | High | Automated daily compatibility suite against sandboxes. |
| **Latency in Joins** | Medium | Strict P50 path via predicate pushdown/broadcast joins. |
| **Compliance (BYOC)** | Medium | Standardized Terraform/K8s patterns for easy VPC deploy. |

## 4. Resource Assumptions
- **Cloud Spend**: ~$5k/mo (Dev/Test) scaling with QPS.
- **Infra**: AWS EKS/Fargate, ElastiCache (Redis), KMS.
