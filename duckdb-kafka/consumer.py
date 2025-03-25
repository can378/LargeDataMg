from kafka import KafkaConsumer
import json
import duckdb

# DuckDB 연결 (메모리맵 or 파일 저장)
con = duckdb.connect('data.duckdb')

# 테이블 생성 (없으면 자동 생성)
con.execute("""
CREATE TABLE IF NOT EXISTS TB_WEB_RACK_MST (
    seq INTEGER,
    Column1 TEXT,
    Column2 INTEGER,
    Column3 INTEGER,
    Column4 TEXT,
    Column5 TEXT
)
""")

consumer = KafkaConsumer(
    'maria-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='duckdb-sync',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Kafka Consumer 실행 중...")

for msg in consumer:
    event = msg.value
    payload = event.get("payload", {})
    print("수신된 데이터:", payload)

    if event["event"] == "insert":
        con.execute("""
            INSERT INTO TB_WEB_RACK_MST (seq, Column1, Column2, Column3, Column4, Column5)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            payload.get("seq"),
            payload.get("Column1"),
            payload.get("Column2"),
            payload.get("Column3"),
            payload.get("Column4"),
            payload.get("Column5")
        ))

        print(f"DuckDB 삽입 완료: seq={payload.get('seq')}")



results = con.execute("SELECT * FROM TB_WEB_RACK_MST").fetchall()
for row in results:
    print(row)
