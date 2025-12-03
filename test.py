from database import get_connection

try:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DATABASE(), VERSION()")
    print("Kết quả:", cur.fetchall())
    conn.close()
    print("Kết nối OK")
except Exception as e:
    print("LỖI KẾT NỐI:", repr(e))
