"""Tests for OIDC validation, RLS, and CLS enforcement."""
import asyncio
import pytest

from omnisql.security.oidc import OIDCValidator, TenantSecurityContext, DEV_TOKEN_MAP
from omnisql.security.enforcer import apply_rls, apply_cls, _mask_pii
from omnisql.tenant.models import TenantConfig, ConnectorConfig, RLSRule, CLSRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _demo_tenant() -> TenantConfig:
    return TenantConfig(
        tenant_id="test",
        display_name="Test",
        connector_configs={
            "github": ConnectorConfig(connector_id="github", base_url="mock"),
            "jira": ConnectorConfig(connector_id="jira", base_url="mock"),
        },
        rls_rules=[
            RLSRule(connector_id="github", rule_expr="team_id == user.team_id"),
            RLSRule(connector_id="jira", rule_expr="project.lower() == user.team_id"),
        ],
        cls_rules=[
            CLSRule(connector_id="github", column="author_email",
                    action="hash_hmac", condition="user.pii_access == false"),
            CLSRule(connector_id="github", column="author",
                    action="block", condition='user.role == "qa"'),
        ],
        table_registry={},
    )


def _make_ctx(tenant: TenantConfig, token_name: str) -> TenantSecurityContext:
    claims = DEV_TOKEN_MAP[token_name]
    return TenantSecurityContext(
        user_id=claims["user_id"],
        email=claims["email"],
        role=claims["role"],
        team_id=claims["team_id"],
        pii_access=claims["pii_access"],
        tenant_id=tenant.tenant_id,
        tenant_cfg=tenant,
    )


MOCK_GITHUB_DATA = [
    {"pr_id": "PR-001", "author": "dev1", "author_email": "dev1@co.com",
     "team_id": "mobile", "status": "open"},
    {"pr_id": "PR-002", "author": "dev2", "author_email": "dev2@co.com",
     "team_id": "web", "status": "merged"},
    {"pr_id": "PR-003", "author": "dev3", "author_email": "dev3@co.com",
     "team_id": "mobile", "status": "merged"},
]

MOCK_JIRA_DATA = [
    {"issue_key": "PRJ-001", "project": "MOBILE", "status": "In Progress",
     "branch_name": "feature/mobile/task-1"},
    {"issue_key": "PRJ-002", "project": "WEB", "status": "Done",
     "branch_name": "feature/web/task-2"},
    {"issue_key": "PRJ-003", "project": "MOBILE", "status": "To Do",
     "branch_name": "feature/mobile/task-3"},
]


# ---------------------------------------------------------------------------
# OIDC Validation (dev token map mode)
# ---------------------------------------------------------------------------

class TestOIDCValidator:
    @pytest.fixture
    def oidc(self):
        return OIDCValidator(jwks_url="", audience="omnisql-dev")

    @pytest.fixture
    def tenant(self):
        return _demo_tenant()

    @pytest.mark.asyncio
    async def test_valid_developer_token(self, oidc, tenant):
        ctx = await oidc.validate("token_dev", tenant)
        assert ctx.user_id == "u1"
        assert ctx.role == "developer"
        assert ctx.team_id == "mobile"
        assert ctx.pii_access is True
        assert ctx.tenant_id == "test"

    @pytest.mark.asyncio
    async def test_valid_qa_token(self, oidc, tenant):
        ctx = await oidc.validate("token_qa", tenant)
        assert ctx.role == "qa"
        assert ctx.pii_access is False

    @pytest.mark.asyncio
    async def test_valid_web_developer_token(self, oidc, tenant):
        ctx = await oidc.validate("token_web_dev", tenant)
        assert ctx.team_id == "web"

    @pytest.mark.asyncio
    async def test_invalid_token_raises_401(self, oidc, tenant):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await oidc.validate("bad_token", tenant)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_security_context_has_tenant_cfg(self, oidc, tenant):
        ctx = await oidc.validate("token_dev", tenant)
        assert ctx.tenant_cfg is tenant

    @pytest.mark.asyncio
    async def test_to_opa_input(self, oidc, tenant):
        ctx = await oidc.validate("token_dev", tenant)
        opa_input = ctx.to_opa_input()
        assert opa_input["user_id"] == "u1"
        assert opa_input["team_id"] == "mobile"
        assert "tenant_id" not in opa_input  # not sent to OPA


# ---------------------------------------------------------------------------
# Row-Level Security
# ---------------------------------------------------------------------------

class TestRowLevelSecurity:
    @pytest.mark.asyncio
    async def test_github_rls_mobile_team(self):
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_dev")  # mobile team
        result = await apply_rls("github", MOCK_GITHUB_DATA, ctx)
        assert all(r["team_id"] == "mobile" for r in result)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_github_rls_web_team(self):
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_web_dev")  # web team
        result = await apply_rls("github", MOCK_GITHUB_DATA, ctx)
        assert all(r["team_id"] == "web" for r in result)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_jira_rls_case_insensitive(self):
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_dev")  # mobile team
        # Jira RLS rule: project.lower() == user.team_id
        result = await apply_rls("jira", MOCK_JIRA_DATA, ctx)
        assert all(r["project"] == "MOBILE" for r in result)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_rls_no_rules_passes_all(self):
        """Connector with no RLS rules returns all data."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_dev")
        # "linear" has no RLS rules
        data = [{"id": "1"}, {"id": "2"}]
        result = await apply_rls("linear", data, ctx)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_rls_empty_data(self):
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_dev")
        result = await apply_rls("github", [], ctx)
        assert result == []


# ---------------------------------------------------------------------------
# Column-Level Security
# ---------------------------------------------------------------------------

class TestColumnLevelSecurity:
    @pytest.mark.asyncio
    async def test_cls_no_masking_for_developer(self):
        """Developer with pii_access=True sees all columns unmasked."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_dev")
        result = await apply_cls("github", MOCK_GITHUB_DATA, ctx)
        assert result[0]["author_email"] == "dev1@co.com"
        assert result[0]["author"] == "dev1"

    @pytest.mark.asyncio
    async def test_cls_email_masking_for_qa(self):
        """QA with pii_access=False gets hashed emails."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_qa")
        result = await apply_cls("github", MOCK_GITHUB_DATA, ctx)
        assert "****@ema.co" in result[0]["author_email"]

    @pytest.mark.asyncio
    async def test_cls_author_blocked_for_qa(self):
        """QA role gets author field blocked."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_qa")
        result = await apply_cls("github", MOCK_GITHUB_DATA, ctx)
        assert result[0]["author"] == "[HIDDEN]"

    @pytest.mark.asyncio
    async def test_cls_masking_consistency(self):
        """Same input always produces same hash."""
        h1 = _mask_pii("dev1@co.com")
        h2 = _mask_pii("dev1@co.com")
        assert h1 == h2
        assert "****@ema.co" in h1

    @pytest.mark.asyncio
    async def test_cls_does_not_mutate_original(self):
        """CLS should not modify the original data list."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_qa")
        data = [{"author": "dev1", "author_email": "dev1@co.com", "team_id": "mobile"}]
        _ = await apply_cls("github", data, ctx)
        assert data[0]["author"] == "dev1"  # original unchanged

    @pytest.mark.asyncio
    async def test_cls_no_rules_passes_all(self):
        """Connector with no CLS rules returns data unmodified."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_qa")
        data = [{"field": "value"}]
        result = await apply_cls("jira", data, ctx)  # no CLS rules for jira
        assert result[0]["field"] == "value"


# ---------------------------------------------------------------------------
# Full pipeline: RLS → CLS
# ---------------------------------------------------------------------------

class TestSecurityPipeline:
    @pytest.mark.asyncio
    async def test_rls_then_cls(self):
        """Full pipeline: filter rows, then mask columns."""
        tenant = _demo_tenant()
        ctx = _make_ctx(tenant, "token_qa")  # mobile QA
        data = await apply_rls("github", MOCK_GITHUB_DATA, ctx)
        data = await apply_cls("github", data, ctx)
        # Only mobile team rows
        assert all(r["team_id"] == "mobile" for r in data)
        # Author blocked, email hashed
        assert all(r["author"] == "[HIDDEN]" for r in data)
        assert all("****@ema.co" in r["author_email"] for r in data)
