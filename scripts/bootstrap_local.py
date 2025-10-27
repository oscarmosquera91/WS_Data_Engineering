import os
import time

try:
    import pymssql
except Exception:
    pymssql = None

try:
    import boto3
    from botocore.client import Config
except Exception:
    boto3 = None


def wait_for_sql(host, port, user, password, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if not pymssql:
                raise RuntimeError("pymssql not installed")
            conn = pymssql.connect(host=host, user=user, password=password, port=int(port))
            conn.close()
            return True
        except Exception:
            time.sleep(5)
    raise TimeoutError("SQL Server did not become available in time")


def create_sample_db(host, port, user, password):
    if not pymssql:
        print("pymssql not available; skipping DB bootstrap")
        return
    conn = pymssql.connect(server=host, user=user, password=password, port=int(port))
    cursor = conn.cursor()
    cursor.execute("IF DB_ID('warehouse_local') IS NULL CREATE DATABASE warehouse_local;")
    cursor.execute("USE warehouse_local; IF OBJECT_ID('dbo.sample_table') IS NULL CREATE TABLE dbo.sample_table (id INT PRIMARY KEY, name NVARCHAR(100));")
    cursor.execute("USE warehouse_local; IF NOT EXISTS (SELECT 1 FROM dbo.sample_table WHERE id=1) INSERT INTO dbo.sample_table (id, name) VALUES (1, 'sample');")
    conn.commit()
    conn.close()
    print("Sample DB and table created.")


def upload_sample_to_minio(endpoint, access_key, secret_key):
    if not boto3:
        print("boto3 not available; skipping MinIO upload")
        return
    s3 = boto3.resource(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    bucket = 'local-lakehouse'
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    sample_key = 'sample/sample.txt'
    s3.Object(bucket, sample_key).put(Body=b'Sample lakehouse file')
    print(f"Uploaded sample to {endpoint}/{bucket}/{sample_key}")


if __name__ == "__main__":
    sql_host = os.getenv("SQL_HOST", "sqlserver")
    sql_port = os.getenv("SQL_PORT", "1433")
    sql_user = os.getenv("SQL_USER", "sa")
    sql_pass = os.getenv("SQL_PASS")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    print("Waiting for SQL Server...")
    try:
        wait_for_sql(sql_host, sql_port, sql_user, sql_pass)
        create_sample_db(sql_host, sql_port, sql_user, sql_pass)
    except Exception as e:
        print(f"SQL bootstrap skipped/error: {e}")

    try:
        upload_sample_to_minio(minio_endpoint, minio_access, minio_secret)
    except Exception as e:
        print(f"MinIO bootstrap skipped/error: {e}")

    print("Bootstrap complete.")
import os
import time
import pymssql
import boto3
from botocore.client import Config

def wait_for_sql(host, port, user, password, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymssql.connect(host=host, user=user, password=password, port=int(port))
            conn.close()
            return True
        except Exception:
            time.sleep(5)
    raise TimeoutError("SQL Server did not become available in time")

def create_sample_db(host, port, user, password):
    conn = pymssql.connect(server=host, user=user, password=password, port=int(port))
    cursor = conn.cursor()
    cursor.execute("IF DB_ID('warehouse_local') IS NULL CREATE DATABASE warehouse_local;")
    cursor.execute("USE warehouse_local; IF OBJECT_ID('dbo.sample_table') IS NULL CREATE TABLE dbo.sample_table (id INT PRIMARY KEY, name NVARCHAR(100));")
    cursor.execute("USE warehouse_local; IF NOT EXISTS (SELECT 1 FROM dbo.sample_table WHERE id=1) INSERT INTO dbo.sample_table (id, name) VALUES (1, 'sample');")
    conn.commit()
    conn.close()
    print("Sample DB and table created.")

def upload_sample_to_minio(endpoint, access_key, secret_key):
    s3 = boto3.resource(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    bucket = 'local-lakehouse'
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    sample_key = 'sample/sample.txt'
    s3.Object(bucket, sample_key).put(Body=b'Sample lakehouse file')
    print(f"Uploaded sample to {endpoint}/{bucket}/{sample_key}")

if __name__ == "__main__":
    sql_host = os.getenv("SQL_HOST", "sqlserver")
    sql_port = os.getenv("SQL_PORT", "1433")
    sql_user = os.getenv("SQL_USER", "sa")
    sql_pass = os.getenv("SQL_PASS")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    print("Waiting for SQL Server...")
    wait_for_sql(sql_host, sql_port, sql_user, sql_pass)
    create_sample_db(sql_host, sql_port, sql_user, sql_pass)
    upload_sample_to_minio(minio_endpoint, minio_access, minio_secret)
    print("Bootstrap complete.")