# OmniSQL: High-Level Design

## 1. Architecture Overview
OmniSQL follows a **Control Plane / Data Plane** separation to decouple management logic from high-throughput query execution.

### System Diagram
```mermaid
graph TD
    User([End User / API Client]) --> Gateway[API Gateway / Auth]
    
    subgraph "Control Plane (Regional)"
        Gateway --> CP[Control Plane Service]
        CP --> Schema[Schema & Connector Registry]
        CP --> TenantDB[(Tenant & Policy Metadata)]
        CP --> KMS[Per-Tenant Key Management]
    end

    subgraph "Data Plane (Stateless, Auto-scaled)"
        Gateway --> Planner[Query Planner & Optimizer]
        Planner --> Executor[Distributed Executor]
        Executor --> Cache[(Result & Metadata Cache)]
        
        subgraph "Connector Layer"
            Executor --> ConA[Salesforce Connector]
            Executor --> ConB[Jira Connector]
            Executor --> ConC[GitHub Connector]
        end
    end

    subgraph "External SaaS Ecosystem"
        ConA --> SF[Salesforce API]
        ConB --> JR[Jira API]
        ConC --> GH[GitHub API]
    end

    classDef plane fill:#f9f,stroke:#333,stroke-width:2px;
    class DataPlane plane;
```

## 2. Component Breakdown
- **Control Plane**: Manages connector registries, tenant metadata, and encryption keys (per-tenant KMS integration).
- **Data Plane**: A stateless fleet handling SSL, SQL parsing (Query Planner), and parallel execution (Distributed Executor).
- **Connector Fleet**: Specialized gRPC adapters (Sidecar Model) that translate SQL predicates into SaaS-native filters (JQL, SOQL, etc.).

## 3. Multi-Tenant Isolation
OmniSQL utilizes **Logical Isolation with Physical Guards**:
- **At Rest**: Tenant credentials and cache are encrypted with unique per-tenant keys.
- **In Flight**: Every context is tagged with a `tenant_id`.
- **Compute**: Workers are pooled but resource-tracked per tenant. High-tier customers can use dedicated worker pools.

## 4. Security & Entitlements
OmniSQL implements a **Least-Privilege** model by default:
- **Passthrough Authentication**: The system forwards the user's OAuth tokens to SaaS providers, ensuring the query respects their existing permissions.
- **Entitlement Proxy**: For service-account-based access, OmniSQL applies an OPA (Open Policy Agent) layer that rewrites SQL queries to include RLS (Row-Level Security) and CLS (Column-Level Security) predicates based on tenant policies.
- **Data Privacy**: PII is masked or redacted by default unless the querying user has explicit `PII_ACCESS` roles.
- **Encryption**: All credentials and sensitive metadata are stored in a dedicated KMS-backed vault, with per-tenant encryption keys derived from a master HSM.

## 5. Governance: Rate Limits & Freshness
- **Rate-Limit Fairness**:
  - **Connector-Level**: Global limits imposed by the SaaS provider (e.g., 100 req/sec for Salesforce).
  - **Tenant-Level**: Prevents a single tenant from exhausting the global connector budget.
  - **User-Level**: Prevents a single user from "slamming" the API with complex joins.
- **Freshness Model**:
  - **On-Demand (Default)**: Always queries the source.
  - **Adaptive Caching**: Result sets are cached with a TTL defined by the connector's "materiality" policy (e.g., GitHub issues cache for 5s, Salesforce Leads for 1min).
  - **Staleness Hints**: Users can pass `--staleness 5m` in SQL to allow queries against a stale cache, reducing latency and rate-limit consumption.

## 6. Cost Control
- **Query Credits**: Tenants are assigned query credits. Heavier queries (e.g., cross-app joins) consume more credits based on compute and API calls.
- **Guardrails**: Automated query cancellation for queries projected to exceed resource limits (e.g., joining two 1M+ row tables without appropriate filters).

## 7. Deployment Modes
### Multi-tenant SaaS
- Shared compute and storage in a managed environment.
- Automated sharding and failover across multiple availability zones.

### Single-tenant / BYOC
- **Data Plane in Customer VPC**: The query executor and connectors run in the customer's Kubernetes cluster (AWS EKS, GCP GKE).
- **Managed Control Plane**: Policy management, schema registry, and audit logging remain in the provider's cloud for central governance.
- **Zero-Data Leakage**: Customer data never leaves their VPC; only metadata and control signals are exchanged with the Control Plane.

## 8. Scalability Targets
- **Peak QPS**: 1,000 queries per second.
- **Concurrent Users**: 10,000 active sessions.
- **Data Throughput**: 100 MB/s aggregate.
- **Latency SLO**:
  - **P50**: < 500ms (Single source, predicate pushdown).
  - **P95**: < 1.5s (Complex joins, cold cache).
