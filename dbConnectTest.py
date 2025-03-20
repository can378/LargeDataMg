import pymysql

# MariaDB 연결
conn = pymysql.connect(host='127.0.0.1', port=3305, user='test', password='test', database='test')


print("DB 연결 성공!")