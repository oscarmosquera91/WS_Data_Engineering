import os
from typing import Any, Dict, Optional


class KeyVaultClientProtocol:
    """Protocol-like minimal KeyVault client interface expected by ConnectionManager.

    The real implementation should provide a `get_secret(name: str) -> str` method.
    """

    def get_secret(self, name: str) -> str:  # pragma: no cover - interface
        raise NotImplementedError()


class ConnectionManager:
    """Resuelve conexiones lógicas a configuraciones por entorno.

    Comportamiento clave:
    - Usa `env` (pasado o `ENV` env var) para seleccionar la sección (local|fabric).
    - Si en la configuración existe `endpoint_env`, se resuelve leyendo la variable de entorno
      indicada y se expone como `endpoint` en la resolución final.
    - Si existe `secret_ref` con prefijo `kv://name`, y se entrega un `keyvault_client`, se
      resuelve y se devuelve en `resolved_secret`.
    """

    def __init__(self, config: Dict[str, Any], env: Optional[str] = None, keyvault_client: Optional[KeyVaultClientProtocol] = None):
        self.config = config
        self.env = env or os.getenv("ENV") or config.get("default_env", "local")
        self.kv = keyvault_client

    def resolve(self, logical_name: str) -> Dict[str, Any]:
        conns = self.config.get("connections", {})
        entry = conns.get(logical_name)
        if not entry:
            raise KeyError(f"Connection '{logical_name}' not found in config")

        env_cfg = entry.get(self.env)
        if env_cfg is None:
            raise KeyError(f"No configuration for env '{self.env}' in connection '{logical_name}'")

        # Make a shallow copy to avoid mutating original config
        resolved = dict(env_cfg)

        # Resolve endpoint_env -> endpoint (read from OS env)
        endpoint_env_key = resolved.pop("endpoint_env", None)
        if endpoint_env_key:
            endpoint_val = os.getenv(endpoint_env_key)
            if endpoint_val:
                resolved["endpoint"] = endpoint_val

        # Resolve any keys with suffix _env -> fill from env
        for k, v in list(resolved.items()):
            if k.endswith("_env") and isinstance(v, str):
                real_key = k[: -4]
                resolved_val = os.getenv(v)
                if resolved_val is not None:
                    resolved[real_key] = resolved_val

        # Resolve KeyVault secret references like: kv://secret-name
        secret_ref = resolved.get("secret_ref")
        if secret_ref and isinstance(secret_ref, str) and secret_ref.startswith("kv://"):
            if not self.kv:
                # Do not fail hard: leave unresolved but signal absence
                resolved["resolved_secret"] = None
            else:
                secret_name = secret_ref.replace("kv://", "", 1)
                try:
                    secret_val = self.kv.get_secret(secret_name)
                    resolved["resolved_secret"] = secret_val
                except Exception:
                    resolved["resolved_secret"] = None

        return resolved


__all__ = ["ConnectionManager", "KeyVaultClientProtocol"]
