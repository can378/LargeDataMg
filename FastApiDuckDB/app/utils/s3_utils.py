import duckdb
import boto3
import io
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from threading import Thread
import time

app = FastAPI()

db = duckdb.connect(database=':memory:')
TABLE_NAME = "items"
S3_BUCKET = "hugehugebucket"
S3_KEY = "parquet/output.parquet"

s3_client = boto3.client("s3")

def load_parquet_from_s3():
    print("[INFO] Loading data from S3 into DuckDB")
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    parquet_bytes = response['Body'].read()
    buffer = io.BytesIO(parquet_bytes)

    # DuckDB 테이블 초기화
    db.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    db.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM read_parquet(buffer)", {'buffer': buffer})
    print("[INFO] Data loaded into DuckDB")

def watch_s3_and_reload(interval=60):
    last_modified = None
    while True:
        obj = s3_client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        current = obj['LastModified']

        if last_modified is None or current != last_modified:
            load_parquet_from_s3()
            last_modified = current

        time.sleep(interval)

@app.on_event("startup")
def startup():
    load_parquet_from_s3()
    Thread(target=watch_s3_and_reload, args=(30,), daemon=True).start()

@app.get("/items")
def get_items():
    start = time.time()
    result = db.execute(f"SELECT * FROM {TABLE_NAME}").fetchall()
    columns = [desc[0] for desc in db.description]
    data = [dict(zip(columns, row)) for row in result]
    duration = time.time() - start
    return JSONResponse({
        "duration": f"{duration:.3f}s",
        "count": len(data),
        "items": data
    })
