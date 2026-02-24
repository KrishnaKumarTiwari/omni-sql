from __future__ import annotations
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class OPAClient:
    """
    Stub OPA client — inline fallback mode only (Phase 1).

    OPA HTTP integration is deferred to M3 as per the execution plan.
    When OPA_URL is set in M3, this class will make async HTTP calls to
    the OPA sidecar at http://localhost:8181/v1/data/{policy_path}.

    For now, self._enabled = False ensures the enforcer always uses
    the inline rule evaluator from tenant_cfg.rls_rules / cls_rules.
    """

    def __init__(self, opa_url: str = "") -> None:
        self._opa_url = opa_url
        self._enabled = False   # inline fallback only in this phase

        if opa_url:
            logger.warning(
                "OPA_URL is set (%s) but OPA HTTP integration is not yet implemented. "
                "Falling back to inline rule evaluation.",
                opa_url,
            )

    async def evaluate(self, policy_path: str, input_data: Dict[str, Any]) -> Any:
        """
        Evaluate an OPA policy.

        Currently always raises NotImplementedError because _enabled=False.
        SecurityEnforcer checks self._enabled before calling this.
        """
        raise NotImplementedError("OPA HTTP integration not yet implemented")

    async def close(self) -> None:
        pass
