import os
import pytest

from utils.connection_manager import ConnectionManager


RUN_INT = os.getenv("RUN_INTEGRATION", "0") == "1"


def _require_external_libs():
    try:
        import pymssql  # noqa: F401
        import boto3  # noqa: F401
    except Exception:
        pytest.skip("pymssql or boto3 not installed in this environment")


@pytest.mark.integration
def test_sql_and_minio_end_to_end():
    if not RUN_INT:
        pytest.skip("Integration tests disabled. Set RUN_INTEGRATION=1 to enable")

    _require_external_libs()

    # config mirrors docs/config.yml
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

    # Ensure env vars point to the local docker compose services
    os.environ.setdefault("LOCAL_WAREHOUSE_USER", "sa")
    os.environ.setdefault("LOCAL_WAREHOUSE_PASS", "Your_Strong!Passw0rd")

    cm = ConnectionManager(config)
    resolved = cm.resolve("warehouse")

    # Try to connect to SQL Server and read the sample row
    import pymssql

    ep = resolved.get("endpoint")
    # simple parse for host/port/database (best-effort)
    # expecting jdbc:sqlserver://localhost:1433;database=warehouse_local
    host = "localhost"
    port = 1433
    database = "warehouse_local"

    conn = pymssql.connect(server=host, user=os.getenv("LOCAL_WAREHOUSE_USER"), password=os.getenv("LOCAL_WAREHOUSE_PASS"), port=port, database=database)
    cur = conn.cursor()
    cur.execute("SELECT name FROM dbo.sample_table WHERE id=1")
    row = cur.fetchone()
    conn.close()

    assert row is not None and row[0] == 'sample'

    # Check MinIO for uploaded sample
    import boto3
    from botocore.client import Config

    s3 = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', config=Config(signature_version='s3v4'))
    objs = s3.list_objects_v2(Bucket='local-lakehouse', Prefix='sample/')
    assert objs.get('KeyCount', 0) > 0
