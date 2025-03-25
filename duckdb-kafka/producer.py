from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 이 데이터가 MariaDB에서 변경된 레코드라고 가정하고 테스트트
data = {
    "event": "insert",
    "table": "TB_WEB_RACK_MST",
    "payload": {
        "seq": 1,
        "Column1": "AR",
        "Column2": 2,
        "Column3": 1,
        "Column4": "48_B",
        "Column5": "21&5&2025-03-13&2025-03-13&대구-사과-2kg&3&5"
    }
}


producer.send('maria-events', value=data)
producer.flush()
print("이벤트 전송 완료")
