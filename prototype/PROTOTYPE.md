# OmniSQL Prototype: Cross-App Query Scenario

This document outlines the scope, technical details, and verification steps for the OmniSQL functional prototype.

## 🎯 Scenario: Relationship Intelligence
**Goal**: Identify high-impact open-source contributors who are also potential sales leads in the CRM.

### The Federated Query
```sql
SELECT 
    gh.username, 
    gh.stars, 
    sf.lead_name, 
    sf.company, 
    sf.lead_score
FROM github.users gh
JOIN salesforce.leads sf ON gh.email = sf.email
WHERE gh.stars > 100 
  AND sf.lead_score > 50
ORDER BY sf.lead_score DESC;
```

## 🏗 Prototype Components

### 1. Mock SaaS Connectors
- **GitHub Connector**: 
  - Simulates the `/users` and `/repos` endpoints.
  - Implements **Predicate Pushdown** for `stars > 100`.
  - Rate limit: 10 requests per minute (simulated).
- **Salesforce Connector**: 
  - Simulates the `Leads` object.
  - Implements **Predicate Pushdown** for `lead_score > 50`.
  - Rate limit: 5 requests per minute (simulated).

### 2. Query Engine (Powered by DuckDB)
- **Federation Logic**: Orchestrates data fetching from connectors.
- **Join Execution**: Performs an in-memory hash join between the filtered datasets.
- **Entitlements Middleware**: 
  - Checks if the user has `READ_GITHUB` and `READ_SALESFORCE` permissions.
  - Implements Row-Level Security (RLS) to filter out sensitive leads (e.g., competitors).

### 3. Governance Layer
- **Token Bucket Rate Limiter**: Tracks usage per-tenant and per-connector.
- **Freshness Control**: 
  - `gh.users` cache TTL: 30 seconds.
  - `sf.leads` cache TTL: 60 seconds.
  - Users can bypass cache using `--bypass-cache` flag.

## 🚀 How to Run the Prototype

### Prerequisites
- Python 3.9+
- `pip install duckdb pandas`

### Execution
```bash
# Run the end-to-end query
python prototype/main.py --query "SELECT ..."

# Test rate limiting (run multiple times)
python prototype/main.py --query "..."

# Test entitlements (using a restricted user)
python prototype/main.py --user restricted_user --query "..."
```

## 🧪 Verification Criteria
1. **Security**: Query fails if the user lacks permissions for one of the joined sources.
2. **Entitlements**: `restricted_user` should not see leads from `company = 'Competitor'`.
3. **Rate Limits**: The 3rd consecutive query within 10 seconds should return a `429 Too Many Requests` simulated error.
4. **Performance**: Federated execution (fetch + join) should complete in < 500ms (ignoring simulated network delays).
5. **Freshness**: Subsequent queries within the TTL should return cached results (indicated in logs).
