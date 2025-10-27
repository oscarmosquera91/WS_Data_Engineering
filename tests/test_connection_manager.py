import os
import pytest

from utils.connection_manager import ConnectionManager


class MockKV:
    def __init__(self, mapping):
        self.mapping = mapping

    def get_secret(self, name: str):
        if name in self.mapping:
            return self.mapping[name]
        raise KeyError(name)


def test_resolve_local_env_vars(tmp_path, monkeypatch):
    # Prepare config similar to docs/config.yml
    config = {
        "default_env": "local",
        "connections": {
            "warehouse": {
                "local": {
                    "endpoint": "jdbc:sqlserver://localhost:1433;database=warehouse_local",
                    "user_env": "LOCAL_WAREHOUSE_USER",
                    "pass_env": "LOCAL_WAREHOUSE_PASS",
                }
            },
            "lakehouse": {
                "local": {
                    "endpoint": "file://./local_lakehouse",
                    "mount_path": "./local_lakehouse",
                }
            },
        },
    }

    monkeypatch.setenv("LOCAL_WAREHOUSE_USER", "sa")
    monkeypatch.setenv("LOCAL_WAREHOUSE_PASS", "pass123")

    cm = ConnectionManager(config)
    resolved = cm.resolve("warehouse")

    assert resolved.get("endpoint").startswith("jdbc:sqlserver"), "endpoint should be present"
    assert resolved.get("user") == "sa"
    assert resolved.get("pass") == "pass123"


def test_resolve_fabric_with_kv(monkeypatch):
    config = {
        "default_env": "fabric",
        "connections": {
            "warehouse": {
                "fabric": {
                    "endpoint_env": "FABRIC_WAREHOUSE_ENDPOINT",
                    "secret_ref": "kv://warehouse-conn-string",
                }
            }
        },
    }

    monkeypatch.setenv("FABRIC_WAREHOUSE_ENDPOINT", "https://fabric.example/warehouse/MAIN")
    mock_kv = MockKV({"warehouse-conn-string": "Server=...;User=..;Password=.."})

    cm = ConnectionManager(config, keyvault_client=mock_kv)
    resolved = cm.resolve("warehouse")

    assert resolved.get("endpoint") == "https://fabric.example/warehouse/MAIN"
    assert resolved.get("resolved_secret") == "Server=...;User=..;Password=.."


def test_missing_connection_raises():
    config = {"connections": {}}
    cm = ConnectionManager(config)
    with pytest.raises(KeyError):
        cm.resolve("nope")
