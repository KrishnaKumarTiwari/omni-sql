# OmniSQL: Universal SQL Across Enterprise Apps

OmniSQL is a high-performance federated query layer designed to query multiple enterprise SaaS applications at scale. It allows developers and data teams to treat their entire SaaS stack as a single, unified SQL database.

## 🚀 Quicklinks
- **High-Level Design**: [docs/DESIGN.md](file:///Users/agentkk/Workspace/aionos/omni-sql/docs/DESIGN.md)
- **Six-Month Execution Plan**: [docs/EXECUTION_PLAN.md](file:///Users/agentkk/Workspace/aionos/omni-sql/docs/EXECUTION_PLAN.md)
- **Prototype Documentation**: [prototype/PROTOTYPE.md](file:///Users/agentkk/Workspace/aionos/omni-sql/prototype/PROTOTYPE.md)

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
The current prototype demonstrates a cross-app join between **GitHub** and **Salesforce** to identify key contributors who are also potential high-value sales leads.

**Sample Query**:
```sql
SELECT gh.username, sf.lead_score, sf.company
FROM github.contributors gh
JOIN salesforce.leads sf ON gh.email = sf.email
WHERE sf.lead_score > 80
```
