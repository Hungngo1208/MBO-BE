# personnel_personnel_notifications.py
from flask import Blueprint, request, jsonify
from database import get_connection  # dùng kết nối có sẵn

personnel_notifications_bp = Blueprint("personnel_notifications", __name__)

def cleanup_old_notifications(conn):
    """
    Xoá thông báo đã đọc quá 30 ngày.
    (Không commit tại đây, để hàm gọi chủ động commit 1 lần)
    """
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM nsh.personnel_notifications
            WHERE status = 'read'
              AND created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
            """
        )
    finally:
        try:
            if cur:
                cur.close()
        except:
            pass


# ============================
# API: Lấy danh sách thông báo theo employee_id
# ============================
@personnel_notifications_bp.route("/personnel/notifications", methods=["POST"])
def get_personnel_notifications_by_employee():
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
                SELECT id, employee_id, content, status, created_at
                FROM nsh.personnel_notifications
                WHERE employee_id = %s AND status = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (employee_id, status, limit),
            )
        else:
            cur.execute(
                """
                SELECT id, employee_id, content, status, created_at
                FROM nsh.personnel_notifications
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
# API: Đánh dấu thông báo đã đọc
# ============================
@personnel_notifications_bp.route("/personnel/notifications/read/<int:notification_id>", methods=["PUT"])
def mark_personnel_notification_as_read(notification_id):
    """
    PUT /personnel/notifications/read/<notification_id>
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT status FROM nsh.personnel_notifications WHERE id = %s LIMIT 1",
            (notification_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Không tìm thấy thông báo với ID đã cho"}), 404

        if (row[0] or "").strip().lower() == "read":
            return jsonify({"message": "Thông báo này đã được đọc trước đó"}), 200

        cur.execute(
            "UPDATE nsh.personnel_notifications SET status = 'read' WHERE id = %s",
            (notification_id,),
        )
        conn.commit()

        # (tuỳ chọn) dọn thông báo cũ sau khi có thay đổi
        cleanup_old_notifications(conn)
        conn.commit()

        return jsonify({
            "message": "Cập nhật trạng thái thành công.",
            "notification_id": notification_id,
            "new_status": "read",
        }), 200

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
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
# API: Đánh dấu thông báo CHƯA ĐỌC
# ============================
@personnel_notifications_bp.route("/personnel/notifications/unread/<int:notification_id>", methods=["PUT"])
def mark_personnel_notification_as_unread(notification_id):
    """
    PUT /personnel/notifications/unread/<notification_id>
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT status FROM nsh.personnel_notifications WHERE id = %s LIMIT 1",
            (notification_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Không tìm thấy thông báo với ID đã cho"}), 404

        if (row[0] or "").strip().lower() == "unread":
            return jsonify({"message": "Thông báo này đã ở trạng thái chưa đọc trước đó"}), 200

        cur.execute(
            "UPDATE nsh.personnel_notifications SET status = 'unread' WHERE id = %s",
            (notification_id,),
        )
        conn.commit()

        return jsonify({
            "message": "Cập nhật trạng thái thành công.",
            "notification_id": notification_id,
            "new_status": "unread",
        }), 200

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
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
@personnel_notifications_bp.route("/personnel/notifications/count/<int:employee_id>", methods=["GET"])
def count_personnel_notifications(employee_id):
    """
    GET /personnel/notifications/count/<employee_id>
    Trả về:
    - total
    - unread
    - read
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='unread' THEN 1 ELSE 0 END) AS unread,
                SUM(CASE WHEN status='read' THEN 1 ELSE 0 END) AS read_count
            FROM nsh.personnel_notifications
            WHERE employee_id = %s
            """,
            (employee_id,),
        )
        row = cur.fetchone() or {"total": 0, "unread": 0, "read_count": 0}

        return jsonify({
            "employee_id": employee_id,
            "total": int(row.get("total") or 0),
            "unread": int(row.get("unread") or 0),
            "read": int(row.get("read_count") or 0),
        }), 200

    except Exception as e:
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
# API: Xoá thông báo
# ============================
@personnel_notifications_bp.route("/personnel/notifications/delete", methods=["POST"])
def delete_personnel_notification():
    """
    Body JSON:
    {
        "notification_id": 123,   // bắt buộc (hoặc "id")
        "employee_id": 456        // tùy chọn, để đảm bảo đúng người
    }
    """
    data = request.get_json(silent=True) or {}
    notification_id = data.get("notification_id") or data.get("id")
    employee_id = data.get("employee_id")

    if not notification_id:
        return jsonify({"error": "Thiếu notification_id"}), 400

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if employee_id:
            cur.execute(
                """
                DELETE FROM nsh.personnel_notifications
                WHERE id = %s AND employee_id = %s
                """,
                (notification_id, employee_id),
            )
        else:
            cur.execute(
                """
                DELETE FROM nsh.personnel_notifications
                WHERE id = %s
                """,
                (notification_id,),
            )

        affected = cur.rowcount
        conn.commit()

        if affected == 0:
            return jsonify({"error": "Không tìm thấy thông báo để xoá"}), 404

        return jsonify({
            "success": True,
            "deleted": affected,
            "notification_id": notification_id,
        }), 200

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
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
# API: Thêm thông báo nhân sự
# ============================
@personnel_notifications_bp.route("/personnel/notifications/add", methods=["POST"])
def add_personnel_notification():
    """
    Body JSON:
    {
        "employee_id": 123,
        "content": "Nội dung thông báo"
    }
    """
    data = request.get_json(silent=True) or {}
    employee_id = data.get("employee_id")
    content = data.get("content")

    if not employee_id or not content:
        return jsonify({"error": "Thiếu employee_id hoặc content"}), 400

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO nsh.personnel_notifications
                (employee_id, content, status, created_at)
            VALUES (%s, %s, 'unread', NOW())
            """,
            (employee_id, content),
        )

        # dọn thông báo đã đọc quá 30 ngày
        cleanup_old_notifications(conn)

        conn.commit()

        return jsonify({
            "success": True,
            "employee_id": employee_id,
            "content": content,
            "status": "unread"
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cur: cur.close()
        except: pass
        try:
            if conn: conn.close()
        except: pass
