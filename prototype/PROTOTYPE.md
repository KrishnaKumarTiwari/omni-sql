# OmniSQL Prototype: "Omni-Bridge"

## Scenario: Cross-App Query
Find GitHub Pull Requests not linked to an active Jira ticket.

### Sample SQL
```sql
SELECT gh.title, gh.author, jira.status
FROM github.pull_requests AS gh
LEFT JOIN jira.issues AS jira ON gh.title LIKE '%' || jira.key || '%'
WHERE gh.repo = 'omnisql/core' AND jira.key IS NULL;
```

## Governance & Enforcement
1. **Auth**: JWT-based tenant identification.
2. **Entitlements**: Executor checks user's GitHub/Jira tokens before fetching.
3. **Rate-Limit Handling**: 
   - Planner estimates cost.
   - Connector uses local token bucket to avoid 429s.
4. **Freshness Control**:
   - `X-OmniSQL-Max-Age` header enforced by Cache Layer.

## API Surface
### POST /v1/query
```json
{
  "sql": "SELECT * FROM github.issues WHERE state = 'open' LIMIT 10",
  "freshness": "60s"
}
```

## Metrics for Correctness
- `execution_latency_ms`: Proves P50/P95 targets.
- `cache_hit_ratio`: Proves freshness efficiency.
- `upstream_api_calls_count`: Proves predicate pushdown effectiveness.
