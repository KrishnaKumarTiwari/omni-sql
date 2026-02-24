from __future__ import annotations
import hashlib
import logging
from typing import Any, Dict, List, Optional

from omnisql.security.opa_client import OPAClient

logger = logging.getLogger(__name__)


async def apply_rls(
    connector_id: str,
    data: List[Dict[str, Any]],
    security_ctx: Any,   # TenantSecurityContext (avoid circular import at module level)
    opa_client: Optional[OPAClient] = None,
) -> List[Dict[str, Any]]:
    """
    Apply row-level security filters to fetched data.

    Strategy:
    1. If OPA is enabled (opa_client._enabled): call OPA per row.
    2. Otherwise: evaluate tenant_cfg.rls_rules inline.

    Supported inline expressions (rule_expr syntax):
      'field == user.attr'           → row[field] == getattr(security_ctx, attr)
      'field.lower() == user.attr'   → case-insensitive match

    The prototype hardcoded: team_id filter for GitHub, project.lower() for Jira.
    Here these rules come from the tenant YAML config, zero code required.
    """
    if opa_client and opa_client._enabled:
        return await _apply_rls_opa(connector_id, data, security_ctx, opa_client)

    return _apply_rls_inline(connector_id, data, security_ctx)


def _apply_rls_inline(
    connector_id: str,
    data: List[Dict[str, Any]],
    security_ctx: Any,
) -> List[Dict[str, Any]]:
    """Evaluate tenant_cfg.rls_rules inline for the given connector."""
    rules = [
        r for r in security_ctx.tenant_cfg.rls_rules
        if r.connector_id == connector_id
    ]
    if not rules:
        return data

    user = security_ctx.to_opa_input()

    def _row_passes(row: Dict[str, Any]) -> bool:
        for rule in rules:
            if not _eval_rule_expr(rule.rule_expr, row, user):
                return False
        return True

    return [row for row in data if _row_passes(row)]


def _eval_rule_expr(
    expr: str, row: Dict[str, Any], user: Dict[str, Any]
) -> bool:
    """
    Minimal inline expression evaluator for RLS rules.

    Supported forms:
      'field == user.attr'
      'field.lower() == user.attr'
      'field != user.attr'

    Deliberately limited — complex logic should use OPA.
    """
    expr = expr.strip()

    # Determine operator
    if " == " in expr:
        lhs_str, rhs_str = expr.split(" == ", 1)
        op = "eq"
    elif " != " in expr:
        lhs_str, rhs_str = expr.split(" != ", 1)
        op = "ne"
    else:
        logger.warning("Unsupported RLS rule expression: %s — defaulting to DENY", expr)
        return False

    lhs_str = lhs_str.strip()
    rhs_str = rhs_str.strip()

    # Resolve LHS (row field)
    if lhs_str.endswith(".lower()"):
        field = lhs_str[: -len(".lower()")]
        lhs_value = str(row.get(field, "")).lower()
    else:
        lhs_value = row.get(lhs_str)

    # Resolve RHS (user attribute)
    if rhs_str.startswith("user."):
        attr = rhs_str[5:]
        rhs_value = user.get(attr)
    else:
        rhs_value = rhs_str.strip("'\"")

    if op == "eq":
        return lhs_value == rhs_value
    else:
        return lhs_value != rhs_value


async def _apply_rls_opa(
    connector_id: str,
    data: List[Dict[str, Any]],
    security_ctx: Any,
    opa_client: OPAClient,
) -> List[Dict[str, Any]]:
    """OPA-based RLS (M3 feature — not yet active)."""
    ns = security_ctx.tenant_cfg.opa_policy_namespace
    policy_path = f"{ns}/{connector_id}/rls/allow"
    kept = []
    for row in data:
        result = await opa_client.evaluate(
            policy_path, {"user": security_ctx.to_opa_input(), "row": row}
        )
        if result:
            kept.append(row)
    return kept


# ---------------------------------------------------------------------------

async def apply_cls(
    connector_id: str,
    data: List[Dict[str, Any]],
    security_ctx: Any,
    opa_client: Optional[OPAClient] = None,
) -> List[Dict[str, Any]]:
    """
    Apply column-level security masking/blocking.

    For each CLSRule in tenant_cfg.cls_rules where connector_id matches:
      1. Evaluate rule.condition against security_ctx (if set).
      2. If condition is True (or absent), apply rule.action to every row:
           'hash_hmac' → SHA-256 prefix masking (same as prototype)
           'block'     → replace with '[HIDDEN]'
           'redact'    → replace with 'REDACTED'
    """
    rules = [
        r for r in security_ctx.tenant_cfg.cls_rules
        if r.connector_id == connector_id
    ]
    if not rules:
        return data

    user = security_ctx.to_opa_input()
    result = []
    for row in data:
        row = dict(row)  # shallow copy — don't mutate original
        for rule in rules:
            if not _condition_matches(rule.condition, user):
                continue
            if rule.column not in row:
                continue

            original = row[rule.column]
            if rule.action == "hash_hmac":
                row[rule.column] = _mask_pii(original)
            elif rule.action == "block":
                row[rule.column] = "[HIDDEN]"
            elif rule.action == "redact":
                row[rule.column] = "REDACTED"
        result.append(row)
    return result


def _condition_matches(condition: Optional[str], user: Dict[str, Any]) -> bool:
    """
    Evaluate a condition string against the user dict.

    Supported forms:
      'user.pii_access == false'
      'user.role == "qa"'
      None → always True (rule applies unconditionally)
    """
    if condition is None:
        return True

    condition = condition.strip()
    if " == " not in condition:
        return False

    lhs_str, rhs_str = condition.split(" == ", 1)
    lhs_str = lhs_str.strip()
    rhs_str = rhs_str.strip().strip("\"'")

    if lhs_str.startswith("user."):
        attr = lhs_str[5:]
        actual = user.get(attr)
        # Coerce string "false"/"true" to bool for comparison
        if rhs_str.lower() == "false":
            return actual is False or actual == "false"
        if rhs_str.lower() == "true":
            return actual is True or actual == "true"
        return str(actual) == rhs_str

    return False


def _mask_pii(value: Any) -> str:
    """SHA-256 prefix masking — same algorithm as prototype SecurityEnforcer."""
    if not isinstance(value, str):
        return str(value)
    digest = hashlib.sha256(value.encode()).hexdigest()[:8]
    return f"{digest}****@ema.co"
