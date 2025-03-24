from fastapi import FastAPI
from app.routers import items
from app.utils.duckdb_utils import load_parquet_to_duckdb

app = FastAPI()

# 최초 시작 시 S3에서 파일 다운로드 -> 로컬 parquet 읽기
load_parquet_to_duckdb("parquet/output.parquet")

app.include_router(items.router, prefix="/items")
