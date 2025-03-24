import duckdb

# DuckDB in-memory 연결
con = duckdb.connect(database=':memory:', read_only=False)

# 초기 Parquet 데이터 로드 (추후 S3에서 로드로 변경 가능)
def load_parquet_to_duckdb(parquet_path: str):
    con.execute(f"""
        CREATE OR REPLACE TABLE large_data_table AS 
        SELECT * FROM read_parquet('{parquet_path}')
    """)

def get_connection():
    return con
