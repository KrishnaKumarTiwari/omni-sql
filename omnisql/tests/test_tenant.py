"""Tests for TenantConfig validation and TenantRegistry loading."""
import pytest
import tempfile
import os

from omnisql.tenant.models import TenantConfig, ConnectorConfig, RLSRule, CLSRule
from omnisql.tenant.registry import TenantRegistry


# ---------------------------------------------------------------------------
# TenantConfig validation
# ---------------------------------------------------------------------------

class TestTenantConfig:
    def test_minimal_valid_config(self):
        cfg = TenantConfig(
            tenant_id="t1",
            display_name="Test Tenant",
            connector_configs={
                "github": ConnectorConfig(connector_id="github", base_url="mock"),
            },
        )
        assert cfg.tenant_id == "t1"
        assert cfg.api_budget == 1000  # default
        assert cfg.connector_configs["github"].rate_limit_capacity == 50  # default

    def test_full_config_with_rules(self):
        cfg = TenantConfig(
            tenant_id="acme",
            display_name="Acme Corp",
            api_budget=2000,
            connector_configs={
                "github": ConnectorConfig(
                    connector_id="github",
                    base_url="https://api.github.com",
                    auth_type="bearer",
                    credential_ref="env://GITHUB_TOKEN",
                    rate_limit_capacity=100,
                    rate_limit_refill_rate=15.0,
                    freshness_ttl_ms=30000,
                    pushable_filters=["status", "team_id"],
                    transport="graphql",
                    graphql_path="/graphql",
                ),
            },
            rls_rules=[
                RLSRule(connector_id="github", rule_expr="team_id == user.team_id"),
            ],
            cls_rules=[
                CLSRule(connector_id="github", column="author_email",
                        action="hash_hmac", condition="user.pii_access == false"),
            ],
            table_registry={
                "github.pull_requests": {"connector": "github", "fetch_key": "all_prs"},
            },
        )
        assert cfg.api_budget == 2000
        assert len(cfg.rls_rules) == 1
        assert cfg.cls_rules[0].action == "hash_hmac"
        assert cfg.connector_configs["github"].transport == "graphql"

    def test_connector_config_defaults(self):
        cc = ConnectorConfig(connector_id="test", base_url="mock")
        assert cc.auth_type == "bearer"
        assert cc.transport == "rest"
        assert cc.graphql_path == "/graphql"
        assert cc.page_size == 100
        assert cc.freshness_ttl_ms == 60_000

    def test_invalid_config_missing_required(self):
        with pytest.raises(Exception):
            TenantConfig()  # missing required fields


# ---------------------------------------------------------------------------
# TenantRegistry
# ---------------------------------------------------------------------------

class TestTenantRegistry:
    def test_load_from_configs_dir(self):
        registry = TenantRegistry("configs/tenants")
        registry.load_all()
        assert "demo_tenant" in registry.all_tenant_ids()
        assert "acme_corp" in registry.all_tenant_ids()
        assert registry.count() == 2

    def test_get_existing_tenant(self):
        registry = TenantRegistry("configs/tenants")
        registry.load_all()
        cfg = registry.get("demo_tenant")
        assert cfg is not None
        assert cfg.tenant_id == "demo_tenant"
        assert "github" in cfg.connector_configs
        assert "jira" in cfg.connector_configs

    def test_get_nonexistent_tenant(self):
        registry = TenantRegistry("configs/tenants")
        registry.load_all()
        assert registry.get("nonexistent") is None

    def test_reload(self):
        registry = TenantRegistry("configs/tenants")
        registry.load_all()
        count_before = registry.count()
        registry.reload()
        assert registry.count() == count_before

    def test_missing_dir_raises(self):
        registry = TenantRegistry("/nonexistent/path")
        with pytest.raises(FileNotFoundError):
            registry.load_all()

    def test_yaml_config_has_table_registry(self):
        registry = TenantRegistry("configs/tenants")
        registry.load_all()
        cfg = registry.get("demo_tenant")
        assert "github.pull_requests" in cfg.table_registry
        assert cfg.table_registry["github.pull_requests"]["connector"] == "github"

    def test_yaml_config_has_rls_rules(self):
        registry = TenantRegistry("configs/tenants")
        registry.load_all()
        cfg = registry.get("demo_tenant")
        github_rules = [r for r in cfg.rls_rules if r.connector_id == "github"]
        assert len(github_rules) == 1
        assert "team_id" in github_rules[0].rule_expr
