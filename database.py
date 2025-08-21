import mysql.connector
import os

# Đặt tên schema ở đây (sau này chỉ cần đổi 1 chỗ)
DB_SCHEMA = os.getenv("DB_SCHEMA", "db vcc")

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", "1234"),
        database=DB_SCHEMA,    # database mặc định khi connect
        auth_plugin='mysql_native_password'
    )
