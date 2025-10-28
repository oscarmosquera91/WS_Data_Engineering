import os
from typing import Any, Dict, Optional

try:
    # prefer the existing local ConnectionManager if available
    from utils.connection_manager import ConnectionManager
except Exception:
    ConnectionManager = None


class Adapter:
    """Adapter to abstract Fabric (notebookutils) vs local behavior.

    Usage patterns:
    - In Fabric: instantiate Adapter(notebookutils=nbu) and call get_db_connection(...)
    - Local: Adapter() and call resolve(logical_name) to get resolved connection info
    """

    def __init__(self, notebookutils: Optional[Any] = None, env: Optional[str] = None):
        self.notebookutils = notebookutils
        # env fallback: use ENV or provided env (e.g., 'local' or 'fabric')
        self.env = env or os.getenv("ENV") or "local"
        # local resolver instance (lazy)
        self._local_resolver = None

    @property
    def local_resolver(self):
        if self._local_resolver is None and ConnectionManager is not None:
            # load config from docs/config.yml if needed elsewhere
            # Caller should pass a config dict, but we reuse existing ConnectionManager API
            import yaml
            try:
                cfg_path = os.path.join(os.getcwd(), "docs", "config.yml")
                if os.path.exists(cfg_path):
                    with open(cfg_path, "r", encoding="utf-8") as f:
                        cfg = yaml.safe_load(f)
                else:
                    cfg = {"default_env": self.env, "connections": {}}
            except Exception:
                cfg = {"default_env": self.env, "connections": {}}

            self._local_resolver = ConnectionManager(cfg, env=self.env)
        return self._local_resolver

    def resolve(self, logical_name: str) -> Dict[str, Any]:
        """Resolve a logical connection name into a dict.

        If `notebookutils` is present, return a small descriptor referencing Fabric.
        Otherwise use the local ConnectionManager resolver.
        """
        if self.notebookutils:
            # Fabric path: return a descriptor the caller can use with notebookutils
            return {"type": "fabric", "logical_name": logical_name, "notebookutils": self.notebookutils}

        # Local path
        resolver = self.local_resolver
        if resolver is None:
            raise RuntimeError("Local ConnectionManager not available")

        return resolver.resolve(logical_name)

    def get_db_connection(self, logical_name: str, *, artifact_name: Optional[str] = None, ws_id: Optional[str] = None, warehouse_type: Optional[str] = None):
        """Return either a Fabric connection object (if notebookutils present) or the resolved local config.

        Note: creating a ready-to-use SQLAlchemy engine is left to callers; adapter returns what is practical
        for the current environment (a fabric connection or a dict with endpoint/user/pass).
        """
        if self.notebookutils:
            # Expect caller to pass artifact_name and ws_id for Fabric
            try:
                conn = self.notebookutils.data.connect_to_artifact(artifact_name or logical_name, ws_id, warehouse_type)
                return conn
            except Exception as e:
                raise

        # Local fallback — return resolved dict the caller can use to create an engine
        resolved = self.resolve(logical_name)
        return resolved

    def get_storage_token(self) -> Optional[str]:
        """Return a storage token string when running in Fabric, otherwise None.

        In Fabric the caller typically uses `notebookutils.credentials.getToken("storage")`.
        """
        if not self.notebookutils:
            return None
        try:
            return self.notebookutils.credentials.getToken("storage")
        except Exception:
            return None


__all__ = ["Adapter"]
