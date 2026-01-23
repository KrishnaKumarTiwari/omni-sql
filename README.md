# OmniSQL: Universal SQL Across Enterprise Apps

OmniSQL is a high-performance federated query layer designed to query multiple enterprise SaaS applications at scale. 

## 🚀 Quickstart
- **Design Overview**: [docs/DESIGN.md](docs/DESIGN.md)
- **Execution Plan**: [docs/EXECUTION_PLAN.md](docs/EXECUTION_PLAN.md)
- **Prototype Scenario**: [prototype/PROTOTYPE.md](prototype/PROTOTYPE.md)

## 🎯 Project Rationale & Trade-offs

### 1. Real-time Federated Execution vs. Warehousing (ETL)
**Decision**: Real-time Query Layer.
- **Why**: Modern operational workflows require current state. Warehouses (Snowflake/BigQuery) are excellent for analytics but suffer from data freshness lags (minutes to hours).
- **Trade-off**: Higher complexity in managing heterogeneous SaaS rate limits and network latency.

### 2. Stateless Execution vs. Heavy Caching
**Decision**: Stateless Data Plane with "Just-in-Time" Caching.
- **Why**: Statutory compliance (GDPR/CCPA) is easier if we don't store PII long-term. Statelessness allows for effortless regional sharding.
- **Trade-off**: We accept higher P99 latencies for complex un-cached queries compared to a "pre-indexed" architecture.

### 3. Predicate Pushdown vs. Full Materialization
**Decision**: Prioritize Pushdown.
- **Why**: To hit the P50 < 500ms target, we must minimize data transfer. Fetching 10k rows to filter them in-memory is the primary killer of performance in federated SQL.
- **Trade-off**: SQL capabilities are limited to what the underlying SaaS API supports (e.g., restricted `JOIN` depth).

### 4. Multi-tenant SaaS vs. BYOC
**Decision**: Hybrid "Control/Data Plane" Architecture.
- **Why**: To capture the "Enterprise" market, we must allow the Data Plane to run in the customer's cloud (BYOC).
- **Trade-off**: Significantly higher engineering overhead in maintaining deployment automation across multiple cloud providers.

## 📈 Targets
- **Scale**: 10M Users | 1k QPS
- **Latency**: P50 < 500ms | P95 < 1.5s
- **Connectors**: 1000s (SaaS Sidecar Model)
