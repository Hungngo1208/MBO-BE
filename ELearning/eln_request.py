# eln_request.py
from datetime import datetime
from flask import Blueprint, request, jsonify
from database import get_connection  # dùng kết nối có sẵn

eln_request_bp = Blueprint("eln_request", __name__)

@eln_request_bp.route("/eln/request", methods=["POST"])
def request_course_deadline():
    """
    API cập nhật thời gian yêu cầu học và tạo thông báo cho nhân viên
    """
    data = request.get_json(silent=True) or {}
    employee_id = data.get("employee_id")
    course_id = data.get("course_id")
    tg = data.get("thoi_gian_yeu_cau")

    if not employee_id or not course_id or not tg:
        return jsonify({"error": "Thiếu employee_id, course_id hoặc thoi_gian_yeu_cau"}), 400

    # Parse date
    def parse_date(s):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except Exception:
                pass
        return None

    tg_date = parse_date(tg)
    if tg_date is None:
        return jsonify({"error": "Định dạng ngày không hợp lệ"}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1️⃣ Cập nhật hạn yêu cầu & trạng thái trong eln_employee_courses
        cursor.execute(
            """
            UPDATE nsh.eln_employee_courses
            SET thoi_gian_yeu_cau = %s,
                hien_trang = 'Đã yêu cầu'
            WHERE employee_id = %s AND course_id = %s
            """,
            (tg_date, employee_id, course_id),
        )

        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "Không tìm thấy bản ghi phù hợp trong eln_employee_courses"}), 404

        # 2️⃣ Cập nhật trạng thái tổng trong eln_employee_status
        cursor.execute(
            """
            UPDATE nsh.eln_employee_status
            SET hien_trang = 'Đã yêu cầu'
            WHERE employee_id = %s
            """,
            (employee_id,),
        )

        # 3️⃣ Lấy tên khóa học
        cursor.execute("SELECT title FROM nsh.eln WHERE id = %s LIMIT 1", (course_id,))
        row = cursor.fetchone()
        course_title = row[0] if row else "(Không rõ tên khóa học)"

        # 4️⃣ Tạo thông báo
        tg_text = tg_date.strftime("%d/%m/%Y")
        content = f"Bạn có yêu cầu học môn học {course_title} trước ngày {tg_text}"

        cursor.execute(
            """
            INSERT INTO nsh.eln_notifications (employee_id, content, status)
            VALUES (%s, %s, 'unread')
            """,
            (employee_id, content),
        )

        conn.commit()
        return jsonify({
            "message": "Đã cập nhật thành công cả hai bảng và tạo thông báo mới.",
            "employee_id": employee_id,
            "course_id": course_id,
            "thoi_gian_yeu_cau": tg_date.isoformat(),
            "status": "Đã yêu cầu",
            "notification": content
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor:
                cursor.close()
        except:
            pass
        try:
            if conn:
                conn.close()
        except:
            pass

@eln_request_bp.route("/eln/notifications", methods=["POST"])
def get_notifications_by_employee():
    """
    Body JSON:
    {
        "employee_id": 123,
        "status": "unread" | "read" (tùy chọn),
        "limit": 100 (tùy chọn)
    }
    """
    data = request.get_json(silent=True) or {}
    employee_id = data.get("employee_id")
    status = data.get("status")
    limit = data.get("limit", 100)

    if not employee_id:
        return jsonify({"error": "Thiếu employee_id"}), 400

    try:
        limit = int(limit)
    except Exception:
        limit = 100
    limit = max(1, min(limit, 500))

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        if status in ("unread", "read"):
            cur.execute(
                """
                SELECT id, employee_id, content, status
                FROM nsh.eln_notifications
                WHERE employee_id = %s AND status = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (employee_id, status, limit),
            )
        else:
            cur.execute(
                """
                SELECT id, employee_id, content, status
                FROM nsh.eln_notifications
                WHERE employee_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (employee_id, limit),
            )

        rows = cur.fetchall()
        return jsonify(rows), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cur: cur.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass
# ============================
# API: Đánh dấu thông báo đã đọc
# ============================
@eln_request_bp.route("/eln/notifications/read/<int:notification_id>", methods=["PUT"])
def mark_notification_as_read(notification_id):
    """
    Cập nhật trạng thái thông báo từ 'unread' sang 'read' theo ID.
    Endpoint: PUT /eln/notifications/read/<notification_id>
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Kiểm tra xem thông báo có tồn tại không
        cur.execute(
            "SELECT status FROM nsh.eln_notifications WHERE id = %s LIMIT 1",
            (notification_id,)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Không tìm thấy thông báo với ID đã cho"}), 404

        current_status = row[0]
        if current_status == "read":
            return jsonify({"message": "Thông báo này đã được đọc trước đó"}), 200

        # Cập nhật trạng thái thành 'read'
        cur.execute(
            "UPDATE nsh.eln_notifications SET status = 'read' WHERE id = %s",
            (notification_id,)
        )
        conn.commit()

        return jsonify({
            "message": "Cập nhật trạng thái thành công.",
            "notification_id": notification_id,
            "new_status": "read"
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        try:
            if conn:
                conn.close()
        except:
            pass
# ============================
# API: Đếm số lượng thông báo theo employee_id
# ============================
@eln_request_bp.route("/eln/notifications/count/<int:employee_id>", methods=["GET"])
def count_notifications(employee_id):
    """
    Trả về số lượng thông báo của nhân viên:
    - Tổng số thông báo
    - Số đã đọc
    - Số chưa đọc
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # Tổng
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='unread' THEN 1 ELSE 0 END) AS unread,
                SUM(CASE WHEN status='read' THEN 1 ELSE 0 END) AS read_count
            FROM nsh.eln_notifications
            WHERE employee_id = %s
        """, (employee_id,))
        row = cur.fetchone() or {"total": 0, "unread": 0, "read_count": 0}

        return jsonify({
            "employee_id": employee_id,
            "total": int(row.get("total") or 0),
            "unread": int(row.get("unread") or 0),
            "read": int(row.get("read_count") or 0)
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        try:
            if cur: cur.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass
