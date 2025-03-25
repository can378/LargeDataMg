from fastapi import FastAPI
import duckdb
from fastapi.responses import StreamingResponse
import io
import pandas as pd
import json


app = FastAPI()
con = duckdb.connect("data.duckdb")

@app.get("/records")
def get_records(limit: int = 1000, offset: int = 0):
    df = con.execute(f"""
        SELECT * FROM TB_WEB_RACK_MST 
        LIMIT {limit} OFFSET {offset}
    """).fetchdf()
    return df.to_dict(orient="records")




@app.get("/records/all")
def get_all_records():
    df = con.execute("SELECT * FROM TB_WEB_RACK_MST").fetchdf()
    return df.to_dict(orient="records")  # 느릴 수 있음


@app.get("/records/stream")
def stream_records():
    df = con.execute("SELECT * FROM TB_WEB_RACK_MST").fetchdf()

    def generate():
        yield '['
        for i, row in df.iterrows():
            if i > 0:
                yield ','
            yield json.dumps(row.to_dict())
        yield ']'

    return StreamingResponse(generate(), media_type="application/json")