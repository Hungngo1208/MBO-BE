# eln_request.py
from datetime import datetime
from flask import Blueprint, request, jsonify
from database import get_connection  # dùng kết nối có sẵn

eln_request_bp = Blueprint("eln_request", __name__)

def cleanup_old_notifications(conn):
    """
    Xoá thông báo đã đọc quá 30 ngày.
    Gọi hàm này sau khi tạo thông báo mới.
    """
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM nsh.eln_notifications
            WHERE status = 'read'
              AND created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
            """
        )
        # Không commit ở đây, để hàm gọi chủ động commit một lần.
    finally:
        try:
            if cur:
                cur.close()
        except:
            pass


@eln_request_bp.route("/eln/request", methods=["POST"])
def request_course_deadline():
    """
    API cập nhật thời gian yêu cầu học và tạo thông báo cho nhân viên.
    Thêm logic:
    - Nếu course của nhân viên đang ở trạng thái pass,
      thì "mở lại" môn học (đổi sang fail) và giảm các bộ đếm hoàn thành.
    - Cập nhật training_type theo rule:
        1. Nếu status cũ = 'fail'  -> giữ nguyên training_type
        2. Nếu status cũ = 'pass' và training_type cũ = 'Đào tạo lại'
           -> giữ nguyên training_type
        3. Nếu status cũ = 'pass' và training_type cũ = 'Đào tạo lần đầu'
           -> đổi training_type thành 'Đào tạo lại'
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

        # === 0) Lấy thông tin hiện tại của course theo employee_id & course_id
        cursor.execute(
            """
            SELECT status, training_type
            FROM nsh.eln_employee_courses
            WHERE employee_id = %s AND course_id = %s
            LIMIT 1
            """,
            (employee_id, course_id),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "Không tìm thấy bản ghi phù hợp trong eln_employee_courses"}), 404

        current_status_raw = row[0] or ""
        current_status = current_status_raw.strip().lower()
        current_training_type_raw = row[1] or ""
        current_training_type = current_training_type_raw.strip()  # có thể là 'Đào tạo lần đầu' / 'Đào tạo lại' / rỗng

        reopened_from_pass = (current_status == "pass")

        # === Xác định training_type mới theo rule bạn yêu cầu ===
        # 1) Nếu status cũ = fail  -> giữ nguyên training_type
        # 2) Nếu status cũ = pass và training_type cũ = 'Đào tạo lại' -> giữ nguyên
        # 3) Nếu status cũ = pass và training_type cũ = 'Đào tạo lần đầu' -> đổi thành 'Đào tạo lại'
        if current_status == "pass":
            if current_training_type == "Đào tạo lần đầu":
                training_type_value = "Đào tạo lại"
            else:
                # 'Đào tạo lại' hoặc giá trị khác -> giữ nguyên
                training_type_value = current_training_type
        else:
            # status khác 'pass' (fail, None, ...) -> giữ nguyên training_type
            training_type_value = current_training_type

        # === 1) Cập nhật eln_employee_courses theo điều kiện

        if reopened_from_pass:
            cursor.execute(
                """
                UPDATE nsh.eln_employee_courses
                SET thoi_gian_yeu_cau = %s,
                    gan_nhat = 'Chưa đào tạo',
                    hien_trang = 'Chưa hoàn thành',
                    ket_qua = NULL,
                    status = 'fail',
                    status_watch = 'fail',
                    training_type = %s
                WHERE employee_id = %s AND course_id = %s
                """,
                (tg_date, training_type_value, employee_id, course_id),
            )
        else:
            cursor.execute(
                """
                UPDATE nsh.eln_employee_courses
                SET thoi_gian_yeu_cau = %s,
                    hien_trang = 'Đã yêu cầu',
                    ket_qua = NULL,
                    status_watch = 'fail',
                    training_type = %s
                WHERE employee_id = %s AND course_id = %s
                """,
                (tg_date, training_type_value, employee_id, course_id),
            )

        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "Không tìm thấy bản ghi phù hợp trong eln_employee_courses"}), 404

        # === 2) Cập nhật trạng thái tổng trong eln_employee_status
        # - Luôn set hien_trang = 'Đã yêu cầu'
        # - Nếu reopen từ pass -> giảm so_mon_hoc_hoan_thanh (không âm)
        if reopened_from_pass:
            cursor.execute(
                """
                UPDATE nsh.eln_employee_status
                SET hien_trang = 'Đã yêu cầu',
                    so_mon_hoc_hoan_thanh = GREATEST(COALESCE(so_mon_hoc_hoan_thanh,0) - 1, 0)
                WHERE employee_id = %s
                """,
                (employee_id,),
            )
        else:
            cursor.execute(
                """
                UPDATE nsh.eln_employee_status
                SET hien_trang = 'Đã yêu cầu'
                WHERE employee_id = %s
                """,
                (employee_id,),
            )

        # === 3) Lấy tên khóa học để ghi thông báo
        cursor.execute("SELECT title FROM nsh.eln WHERE id = %s LIMIT 1", (course_id,))
        row = cursor.fetchone()
        course_title = row[0] if row else "(Không rõ tên khóa học)"

        # === 4) Nếu reopen từ pass -> giảm so_nhan_vien_hoan_thanh của khóa học
        if reopened_from_pass:
            cursor.execute(
                """
                UPDATE nsh.eln
                SET so_nhan_vien_hoan_thanh = GREATEST(COALESCE(so_nhan_vien_hoan_thanh,0) - 1, 0)
                WHERE id = %s
                """,
                (course_id,),
            )

        # === 5) Tạo thông báo
        tg_text = tg_date.strftime("%d/%m/%Y")
        content = f"Bạn có yêu cầu học môn học {course_title} trước ngày {tg_text}"

        cursor.execute(
            """
            INSERT INTO nsh.eln_notifications (employee_id, content, status, created_at)
            VALUES (%s, %s, 'unread', NOW())
            """,
            (employee_id, content),
        )

        # Sau khi tạo thông báo mới -> xoá các thông báo đã đọc quá 30 ngày
        cleanup_old_notifications(conn)

        conn.commit()
        return jsonify({
            "message": "Đã cập nhật thành công và tạo thông báo mới.",
            "employee_id": employee_id,
            "course_id": course_id,
            "thoi_gian_yeu_cau": tg_date.isoformat(),
            "status_overview": "Đã yêu cầu",
            "reopened_from_pass": reopened_from_pass,
            "training_type": training_type_value,
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
        except:
            pass
        try:
            if conn: conn.close()
        except:
            pass

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
# API: Đánh dấu thông báo CHƯA ĐỌC
# ============================
@eln_request_bp.route("/eln/notifications/unread/<int:notification_id>", methods=["PUT"])
def mark_notification_as_unread(notification_id):
    """
    Cập nhật trạng thái thông báo về 'unread' theo ID.
    Endpoint: PUT /eln/notifications/unread/<notification_id>
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 1) Kiểm tra thông báo có tồn tại không
        cur.execute(
            "SELECT status FROM nsh.eln_notifications WHERE id = %s LIMIT 1",
            (notification_id,)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Không tìm thấy thông báo với ID đã cho"}), 404

        current_status = row[0]

        # 2) Idempotent
        if current_status == "unread":
            return jsonify({"message": "Thông báo này đã ở trạng thái chưa đọc trước đó"}), 200

        # 3) Cập nhật về 'unread'
        cur.execute(
            "UPDATE nsh.eln_notifications SET status = 'unread' WHERE id = %s",
            (notification_id,)
        )
        conn.commit()

        return jsonify({
            "message": "Cập nhật trạng thái thành công.",
            "notification_id": notification_id,
            "new_status": "unread"
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
@eln_request_bp.route("/eln/notifications/delete", methods=["POST"])
def delete_notification():
    """
    Body JSON:
    {
        "notification_id": 123,   // bắt buộc
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

        # Nếu có truyền employee_id thì check luôn cho chắc
        if employee_id:
            cur.execute(
                """
                DELETE FROM nsh.eln_notifications
                WHERE id = %s AND employee_id = %s
                """,
                (notification_id, employee_id),
            )
        else:
            cur.execute(
                """
                DELETE FROM nsh.eln_notifications
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
            "notification_id": notification_id
        }), 200

    except Exception as e:
        try:
            if conn:
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
