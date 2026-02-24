from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import HTTPException

from omnisql.tenant.models import TenantConfig

logger = logging.getLogger(__name__)

# Dev token map: matches prototype/utils/security.py exactly so all
# existing tests pass without modification when JWKS_URL is not set.
DEV_TOKEN_MAP: Dict[str, Dict[str, Any]] = {
    "token_dev": {
        "user_id": "u1",
        "email": "dev@company.com",
        "role": "developer",
        "team_id": "mobile",
        "pii_access": True,
    },
    "token_qa": {
        "user_id": "u2",
        "email": "qa@company.com",
        "role": "qa",
        "team_id": "mobile",
        "pii_access": False,
    },
    "token_web_dev": {
        "user_id": "u3",
        "email": "webdev@company.com",
        "role": "developer",
        "team_id": "web",
        "pii_access": True,
    },
}


class OIDCValidator:
    """
    Validates Bearer tokens and returns a TenantSecurityContext.

    Dev mode (jwks_url == "" or None): looks up token in DEV_TOKEN_MAP.
    This preserves all prototype tokens (token_dev, token_qa, token_web_dev)
    so existing integration tests and the web console continue to work.

    Production mode (jwks_url set): validates JWT signature via JWKS.
    Requires python-jose[cryptography] and httpx. JWKS keys are cached
    in-memory with a 1-hour TTL to avoid hammering the IdP.
    """

    JWKS_CACHE_TTL_S = 3600

    def __init__(self, jwks_url: str = "", audience: str = "omnisql") -> None:
        self._jwks_url = jwks_url
        self._audience = audience
        self._dev_mode = not jwks_url
        self._jwks_cache: Optional[Dict] = None
        self._jwks_fetched_at: float = 0.0

        if self._dev_mode:
            logger.warning(
                "OIDCValidator running in DEV MODE — "
                "JWKS_URL not set, using prototype token map. "
                "Do NOT use this in production."
            )

    async def validate(
        self, token: str, tenant_cfg: TenantConfig
    ) -> "TenantSecurityContext":
        """
        Validate token and return a TenantSecurityContext.

        Raises:
            HTTPException(401): token invalid or unknown.
        """
        if self._dev_mode:
            return self._validate_dev(token, tenant_cfg)
        return await self._validate_oidc(token, tenant_cfg)

    # ------------------------------------------------------------------
    # Dev mode (prototype token map)
    # ------------------------------------------------------------------

    def _validate_dev(
        self, token: str, tenant_cfg: TenantConfig
    ) -> "TenantSecurityContext":
        claims = DEV_TOKEN_MAP.get(token)
        if not claims:
            raise HTTPException(status_code=401, detail="Invalid token")
        return TenantSecurityContext(
            user_id=claims["user_id"],
            email=claims["email"],
            role=claims["role"],
            team_id=claims["team_id"],
            pii_access=claims["pii_access"],
            tenant_id=tenant_cfg.tenant_id,
            tenant_cfg=tenant_cfg,
        )

    # ------------------------------------------------------------------
    # Production OIDC (RS256/ES256 JWT + JWKS)
    # ------------------------------------------------------------------

    async def _validate_oidc(
        self, token: str, tenant_cfg: TenantConfig
    ) -> "TenantSecurityContext":
        """Validate JWT against JWKS endpoint. Requires httpx + python-jose."""
        try:
            import time
            from jose import JWTError, jwt as jose_jwt

            jwks = await self._get_jwks()
            # Decode without verification first to get kid
            unverified = jose_jwt.get_unverified_header(token)
            kid = unverified.get("kid")

            # Find matching key
            key = next(
                (k for k in jwks.get("keys", []) if k.get("kid") == kid), None
            )
            if not key:
                # kid miss → refresh JWKS once and retry
                self._jwks_cache = None
                jwks = await self._get_jwks()
                key = next(
                    (k for k in jwks.get("keys", []) if k.get("kid") == kid), None
                )
            if not key:
                raise HTTPException(401, "Token signing key not found")

            claims = jose_jwt.decode(
                token,
                key,
                algorithms=["RS256", "ES256"],
                audience=self._audience,
            )

            return TenantSecurityContext(
                user_id=claims.get("sub", ""),
                email=claims.get("email", ""),
                role=claims.get("role", "viewer"),
                team_id=claims.get("team_id", ""),
                pii_access=bool(claims.get("pii_access", False)),
                tenant_id=tenant_cfg.tenant_id,
                tenant_cfg=tenant_cfg,
            )
        except Exception as exc:
            if isinstance(exc, HTTPException):
                raise
            raise HTTPException(status_code=401, detail=f"Token validation failed: {exc}")

    async def _get_jwks(self) -> Dict:
        """Fetch JWKS from IdP, cached for JWKS_CACHE_TTL_S seconds."""
        import time
        import httpx

        if (
            self._jwks_cache is not None
            and time.time() - self._jwks_fetched_at < self.JWKS_CACHE_TTL_S
        ):
            return self._jwks_cache

        async with httpx.AsyncClient() as client:
            resp = await client.get(self._jwks_url, timeout=5.0)
            resp.raise_for_status()
            self._jwks_cache = resp.json()
            self._jwks_fetched_at = time.time()

        return self._jwks_cache


@dataclass
class TenantSecurityContext:
    """
    Immutable, request-scoped security context.

    Created once by OIDCValidator.validate() and threaded as an explicit
    parameter through every downstream call. No global state.
    """

    user_id: str
    email: str
    role: str
    team_id: str
    pii_access: bool
    tenant_id: str
    tenant_cfg: TenantConfig

    def to_opa_input(self) -> Dict[str, Any]:
        """Serialize user fields for OPA policy evaluation input."""
        return {
            "user_id": self.user_id,
            "email": self.email,
            "role": self.role,
            "team_id": self.team_id,
            "pii_access": self.pii_access,
        }
