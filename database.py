import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="10.73.132.100",
        user="root",
        password="1234",
        database="db qlda",
        auth_plugin='mysql_native_password'  # 👈 Thêm dòng này

    )
