# ELearning/eln_courses.py
from flask import Blueprint, request, jsonify
from mysql.connector import Error
from database import get_connection
from datetime import date, datetime
from decimal import Decimal

eln_courses_bp = Blueprint("eln_courses", __name__)

# =============================
# Các cột gốc của bảng nsh.eln
# =============================
COLUMNS = [
    "id", "title", "positions", "training_time", "note",
    "video_path", "cover_path", "created_at", "updated_at",
    "tong_nhan_vien_hoc", "so_nhan_vien_hoan_thanh"
]


# -----------------------------
# Các hàm tiện ích xử lý JSON
# -----------------------------
def _to_jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return int(v) if v == int(v) else float(v)
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    if isinstance(v, set):
        return sorted(list(v))
    if isinstance(v, list) and len(v) == 1:
        return v[0]
    return v


def _serialize_row(row: dict) -> dict:
    return {k: _to_jsonable(v) for k, v in row.items()}


# -----------------------------
# Hàm phụ: Lấy vị trí nhân viên
# -----------------------------
def _get_position_by_employee_id(conn, employee_id: int):
    sql = """
        SELECT vi_tri
        FROM nsh.employees2026_base
        WHERE id = %s
        LIMIT 1
    """
    with conn.cursor(dictionary=True) as cur:
        cur.execute(sql, (employee_id,))
        row = cur.fetchone()
        if not row:
            return None
        return (row.get("vi_tri") or "").strip()


# ====================================================
# API chính: Lấy danh sách khóa học theo nhân viên
# ====================================================
@eln_courses_bp.route("/eln/courses/by-employee", methods=["GET"])
def get_courses_by_employee():
    employee_id = request.args.get("employee_id", type=int)
    if not employee_id:
        return jsonify({"error": "Thiếu 'employee_id'"}), 400

    conn = None
    try:
        conn = get_connection()

        # 1️⃣ Lấy vị trí công việc của nhân viên
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT vi_tri
                FROM nsh.employees2026_base
                WHERE id = %s
                LIMIT 1
            """, (employee_id,))
            r = cur.fetchone()

        if not r or not r.get("vi_tri"):
            return jsonify({
                "error": "NOT_FOUND",
                "message": f"Không tìm thấy vi_tri cho employee_id={employee_id}"
            }), 404

        position = (r["vi_tri"] or "").strip()

        # 2️⃣ Lấy danh sách khóa học tương ứng vị trí đó
        columns_select = ", ".join([f"e.{c}" for c in COLUMNS])
        sql = f"""
            SELECT
                {columns_select},
                ec.gan_nhat,
                ec.thoi_gian_yeu_cau,
                ec.status,
                ec.status_watch,
                ec.ket_qua,
                ec.training_type
            FROM nsh.eln e
            LEFT JOIN (
                SELECT 
                    course_id,
                    MAX(gan_nhat) AS gan_nhat,
                    MAX(thoi_gian_yeu_cau) AS thoi_gian_yeu_cau,
                    MAX(status) AS status,
                    MAX(status_watch) AS status_watch,
                    MAX(ket_qua) AS ket_qua,
                    MAX(training_type) AS training_type
                FROM nsh.eln_employee_courses
                WHERE employee_id = %s
                GROUP BY course_id
            ) ec ON ec.course_id = e.id
            WHERE
                (
                    (JSON_VALID(e.positions) AND JSON_CONTAINS(e.positions, JSON_QUOTE(%s)))
                    OR
                    (FIND_IN_SET(%s, REPLACE(e.positions, ' ', '')) > 0)
                    OR
                    (TRIM(e.positions) = %s)
                )
            ORDER BY e.created_at DESC, e.id DESC
        """
        params = [employee_id, position, position, position]

        with conn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []

        # 3️⃣ Trả danh sách kết quả (không bao giờ null)
        return jsonify([_serialize_row(x) for x in rows]), 200

    except Exception as e:
        return jsonify({"error": "SERVER_ERROR", "message": str(e)}), 500

    finally:
        if conn:
            conn.close()
# ====================================================
# API: Lịch sử bài kiểm tra theo nhân viên & khóa học
# ====================================================
@eln_courses_bp.route("/eln/quiz-submissions", methods=["GET"])
def get_quiz_submissions_by_employee_and_course():
    """
    Query params (bắt buộc):
      - employee_id: int
      - course_id: int

    Trả về danh sách (có thể rỗng) các lần nộp bài kiểm tra:
      [ { id, employee_id, course_id, status, ket_qua, submitted_at }, ... ]
    """
    employee_id = request.args.get("employee_id", type=int)
    course_id = request.args.get("course_id", type=int)

    if not employee_id or not course_id:
        return jsonify({
            "error": "Thiếu tham số",
            "message": "Cần có 'employee_id' và 'course_id'"
        }), 400

    conn = None
    try:
        conn = get_connection()
        sql = """
            SELECT
                id,
                employee_id,
                course_id,
                status,
                ket_qua,
                submitted_at
            FROM nsh.eln_quiz_submissions
            WHERE employee_id = %s AND course_id = %s
            ORDER BY submitted_at DESC, id DESC
        """
        with conn.cursor(dictionary=True) as cur:
            cur.execute(sql, (employee_id, course_id))
            rows = cur.fetchall() or []

        # Trả luôn mảng (kể cả rỗng)
        return jsonify([_serialize_row(r) for r in rows]), 200

    except Exception as e:
        return jsonify({"error": "SERVER_ERROR", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()
# ====================================================
# API: Đánh dấu status_watch = 'fail'
# ====================================================
@eln_courses_bp.route("/eln/courses/status-watch/fail", methods=["POST"])
def mark_status_watch_fail():
    """
    Body JSON (bắt buộc):
      - employee_id: int
      - course_id: int

    Cập nhật status_watch = 'fail' trong bảng nsh.eln_employee_courses
    cho các bản ghi khớp (employee_id, course_id).
    """
    data = request.get_json(silent=True) or {}
    employee_id = data.get("employee_id")
    course_id = data.get("course_id")

    if not employee_id or not course_id:
        return jsonify({
            "error": "Thiếu tham số",
            "message": "Cần có 'employee_id' và 'course_id' trong body JSON"
        }), 400

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            sql = """
                UPDATE nsh.eln_employee_courses
                SET status_watch = %s
                WHERE employee_id = %s AND course_id = %s
            """
            cur.execute(sql, ("fail", employee_id, course_id))
            affected = cur.rowcount
        conn.commit()

        return jsonify({
            "employee_id": employee_id,
            "course_id": course_id,
            "status_watch": "fail",
            "affected_rows": affected
        }), 200

    except Exception as e:
        return jsonify({"error": "SERVER_ERROR", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()


# ====================================================
# API: Đánh dấu status_watch = 'true'
# ====================================================
@eln_courses_bp.route("/eln/courses/status-watch/true", methods=["POST"])
def mark_status_watch_true():
    data = request.get_json(silent=True) or {}
    employee_id = data.get("employee_id")
    course_id = data.get("course_id")

    if not employee_id or not course_id:
        return jsonify({
            "error": "Thiếu tham số",
            "message": "Cần có 'employee_id' và 'course_id' trong body JSON"
        }), 400

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            sql = """
                UPDATE nsh.eln_employee_courses
                SET status_watch = %s
                WHERE employee_id = %s AND course_id = %s
            """
            # DB chỉ cho 'fail' hoặc 'pass'
            cur.execute(sql, ("pass", employee_id, course_id))
            affected = cur.rowcount
        conn.commit()

        return jsonify({
            "employee_id": employee_id,
            "course_id": course_id,
            "status_watch": "pass",
            "affected_rows": affected
        }), 200

    except Exception as e:
        return jsonify({"error": "SERVER_ERROR", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()
