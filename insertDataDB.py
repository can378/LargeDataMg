import pymysql

# MariaDB 연결
conn = pymysql.connect(host='127.0.0.1', port=3305, user='test', password='test', database='test')
cursor = conn.cursor()

# 데이터 파일 열기
with open("./20250313_대용량 데이터 전송 테스트.txt", "r", encoding="utf-8") as file:
    next(file)  # 1,2 번째 줄 헤더 건너뛰기
    next(file) 
    for line in file:
        parts = line.strip().split('|')  # '|'로 데이터 분리
        if len(parts) < 6:  # 빈 데이터 방지
            continue
        try:
            seq = int(parts[1].strip().replace(',', ''))  # 쉼표 제거 후 변환
        except ValueError:
            print(f"잘못된 숫자 값: {parts[1]}")
            seq = None  # 오류가 발생하면 None 또는 기본값 설정

        column1 = parts[2].strip()
        column2 = int(parts[3].strip())
        column3 = int(parts[4].strip())
        column4 = parts[5].strip()
        column5 = parts[6].strip()  # 여러 개의 값이 포함된 문자열

        # MariaDB에 삽입
        cursor.execute(
            "INSERT INTO LargeData (seq, Column1, Column2, Column3, Column4, Column5) VALUES (%s, %s, %s, %s, %s, %s)",
            (seq, column1, column2, column3, column4, column5)
        )

conn.commit()
conn.close()
