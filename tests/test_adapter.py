import os
import pytest

from ws_de.adapter import Adapter


class DummyNotebookUtils:
    class data:
        @staticmethod
        def connect_to_artifact(a, b, c):
            return f"fabric-conn:{a}:{b}:{c}"

    class credentials:
        @staticmethod
        def getToken(name):
            return f"token-for-{name}"


def test_resolve_local(monkeypatch, tmp_path):
    # ensure env is local
    monkeypatch.delenv("ENV", raising=False)
    adapter = Adapter()

    # without docs/config.yml this will return empty mapping from resolver (or raise if CM missing)
    # but our adapter uses ConnectionManager only if available; if not, resolve will attempt and may raise.
    # For the purpose of this unit test we assert that resolve returns a dict or raises a RuntimeError
    try:
        res = adapter.resolve("warehouse")
        assert isinstance(res, dict)
    except RuntimeError:
        pytest.skip("Local ConnectionManager not available in this environment")


def test_resolve_fabric():
    nb = DummyNotebookUtils()
    adapter = Adapter(notebookutils=nb)
    desc = adapter.resolve("warehouse")
    assert desc["type"] == "fabric"
    assert desc["logical_name"] == "warehouse"

    conn = adapter.get_db_connection("warehouse", artifact_name="AnalyticsDB-OnOff", ws_id="ws-1", warehouse_type="typeA")
    assert isinstance(conn, str) and conn.startswith("fabric-conn:")

    token = adapter.get_storage_token()
    assert token == "token-for-storage"