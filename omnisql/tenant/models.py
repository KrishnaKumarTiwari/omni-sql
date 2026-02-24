from __future__ import annotations
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class ConnectorConfig(BaseModel):
    """Per-connector configuration scoped to a single tenant."""

    connector_id: str
    base_url: str
    auth_type: str = "bearer"          # 'bearer' | 'basic' | 'oauth2'
    credential_ref: str = ""           # 'env://VAR_NAME' or Vault path
    transport: str = "rest"            # 'rest' | 'graphql'
    graphql_path: str = "/graphql"     # appended to base_url for GraphQL

    rate_limit_capacity: int = 50
    rate_limit_refill_rate: float = 10.0
    freshness_ttl_ms: int = 60_000

    pushable_filters: List[str] = Field(default_factory=list)
    # Fields this connector can filter server-side (pushed to the API).
    # Predicates on non-listed fields stay in DuckDB.

    # Optional GraphQL / REST pagination hints
    page_size: int = 100


class RLSRule(BaseModel):
    """Row-level security rule evaluated inline or via OPA."""

    connector_id: str
    # Simple expression over row fields and user context.
    # Supported syntax:
    #   'field == user.attr'              → exact match
    #   'field.lower() == user.attr'      → case-insensitive
    # Future: full OPA policy path, e.g. 'acme/github/rls/allow'
    rule_expr: str


class CLSRule(BaseModel):
    """Column-level security rule for masking or blocking a field."""

    connector_id: str
    column: str
    action: str          # 'hash_hmac' | 'block' | 'redact'
    condition: Optional[str] = None
    # Guard expression evaluated against user context.
    # If None, rule always applies.
    # Supported: 'user.pii_access == false', 'user.role == "qa"'


class TenantConfig(BaseModel):
    """
    Complete, validated configuration for a single tenant.

    Loaded from a YAML file by TenantRegistry and cached in-memory.
    Every production subsystem (cache, rate limiter, security enforcer)
    scopes its operations to tenant_id so no cross-tenant data leakage
    is possible even in a shared-infrastructure deployment.
    """

    tenant_id: str
    display_name: str

    # Global API call budget across all connectors (calls/minute).
    api_budget: int = 1000

    # OPA policy namespace prefix, e.g. 'acme'.
    # Leave empty to use inline rule evaluation.
    opa_policy_namespace: str = ""

    connector_configs: Dict[str, ConnectorConfig]
    rls_rules: List[RLSRule] = Field(default_factory=list)
    cls_rules: List[CLSRule] = Field(default_factory=list)

    # Maps SQL virtual table names to connector + fetch_key.
    # Example: {"github.pull_requests": {"connector": "github", "fetch_key": "all_prs"}}
    table_registry: Dict[str, Dict] = Field(default_factory=dict)
