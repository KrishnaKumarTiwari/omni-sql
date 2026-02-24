from __future__ import annotations
import logging
import threading
from pathlib import Path
from typing import Dict, Optional

import yaml
from pydantic import ValidationError

from omnisql.tenant.models import TenantConfig

logger = logging.getLogger(__name__)


class TenantRegistry:
    """
    Loads, validates, and serves TenantConfig objects.

    Backend: directory of YAML files (one file per tenant, named {tenant_id}.yaml).
    The registry is process-wide. It is initialized once at startup via load_all()
    and supports hot-reload via reload() (safe to call from a SIGHUP handler or
    a background polling task — uses an atomic dict swap).
    """

    def __init__(self, config_dir: str = "configs/tenants") -> None:
        self._config_dir = Path(config_dir)
        self._configs: Dict[str, TenantConfig] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_all(self) -> None:
        """
        Scan config_dir for *.yaml files and parse each into a TenantConfig.
        Replaces the in-memory cache atomically on success.

        Raises:
            FileNotFoundError: if config_dir does not exist.
        """
        if not self._config_dir.exists():
            raise FileNotFoundError(
                f"Tenant config directory not found: {self._config_dir}"
            )

        new_configs: Dict[str, TenantConfig] = {}
        for yaml_path in sorted(self._config_dir.glob("*.yaml")):
            try:
                raw = yaml.safe_load(yaml_path.read_text())
                cfg = TenantConfig.model_validate(raw)
                new_configs[cfg.tenant_id] = cfg
                logger.info("Loaded tenant config: %s (%s)", cfg.tenant_id, yaml_path.name)
            except (ValidationError, yaml.YAMLError) as exc:
                logger.error("Failed to load tenant config %s: %s", yaml_path, exc)
                raise

        with self._lock:
            self._configs = new_configs

        logger.info("TenantRegistry loaded %d tenant(s).", len(new_configs))

    def get(self, tenant_id: str) -> Optional[TenantConfig]:
        """Return TenantConfig for tenant_id, or None if unknown."""
        with self._lock:
            return self._configs.get(tenant_id)

    def reload(self) -> None:
        """
        Hot-reload all configs from disk without dropping in-flight requests.
        Safe to call concurrently — uses a lock + atomic swap.
        """
        logger.info("Hot-reloading tenant configs from %s", self._config_dir)
        self.load_all()

    def all_tenant_ids(self) -> list[str]:
        """Return sorted list of all registered tenant IDs."""
        with self._lock:
            return sorted(self._configs.keys())

    def count(self) -> int:
        with self._lock:
            return len(self._configs)
