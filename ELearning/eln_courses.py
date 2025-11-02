# ELearning/eln_courses.py
from flask import Blueprint, request, jsonify
from mysql.connector import Error
from database import get_connection
from datetime import date, datetime
from decimal import Decimal

eln_courses_bp = Blueprint("eln_courses", __name__)

# Các cột gốc từ bảng eln
COLUMNS = [
    "id", "title", "positions", "training_time", "note",
    "video_path", "cover_path", "created_at", "updated_at",
    "tong_nhan_vien_hoc", "so_nhan_vien_hoan_thanh"
]

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

def _get_position_by_employee_id(conn, employee_id: int):
    sql = """
        SELECT vi_tri
        FROM employees2026_base
        WHERE id = %s
        LIMIT 1
    """
    with conn.cursor(dictionary=True) as cur:
        cur.execute(sql, (employee_id,))
        row = cur.fetchone()
        if not row:
            return None
        return (row.get("vi_tri") or "").strip()

@eln_courses_bp.route("/eln/courses/by-employee", methods=["GET", "POST"])
def get_courses_by_employee():
    """
    Trả về danh sách khoá học theo vị trí của nhân viên,
    kèm 2 trường từ nsh.eln_employee_courses: gan_nhat, thoi_gian_yeu_cau

    - GET  /eln/courses/by-employee?employee_id=1427
    - POST /eln/courses/by-employee  { "employee_id": 1427 }
    """
    employee_id = request.args.get("employee_id", type=int)
    if not employee_id and request.is_json:
        body = request.get_json(silent=True) or {}
        employee_id = body.get("employee_id")

    if not employee_id:
        return jsonify({"error": "Thiếu 'employee_id'"}), 400

    conn = None
    try:
        conn = get_connection()

        # 1) Lấy vị trí nhân viên
        position = _get_position_by_employee_id(conn, employee_id)
        if not position:
            return jsonify({
                "error": "NOT_FOUND",
                "message": f"Không tìm thấy vi_tri cho employee_id={employee_id}"
            }), 404

        # 2) Lấy danh sách khoá học theo positions
        #    + LEFT JOIN với bảng eln_employee_courses đã group theo course_id
        #    để lấy gan_nhat/thoi_gian_yeu_cau mới nhất cho employee_id đó.
        #
        # Lưu ý JSON/SET: dùng OR để cover cả 2 kiểu lưu positions.
        columns_select = ", ".join([f"e.{c}" for c in COLUMNS])
        sql = f"""
            SELECT
                {columns_select},
                ec.gan_nhat,
                ec.thoi_gian_yeu_cau
            FROM eln AS e
            LEFT JOIN (
                SELECT
                    course_id,
                    MAX(gan_nhat) AS gan_nhat,
                    MAX(thoi_gian_yeu_cau) AS thoi_gian_yeu_cau
                FROM eln_employee_courses
                WHERE employee_id = %s
                GROUP BY course_id
            ) AS ec
                ON ec.course_id = e.id
            WHERE
                (
                    (JSON_VALID(e.positions) AND JSON_CONTAINS(e.positions, %s))
                    OR
                    (NOT JSON_VALID(e.positions) AND e.positions = %s)
                )
            ORDER BY e.created_at DESC, e.id DESC
        """

        # Thứ tự params: employee_id (cho subquery) → JSON_CONTAINS → chuỗi so sánh
        params = [employee_id, f"\"{position}\"", position]

        with conn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []

        # Serialize tất cả trường, bao gồm gan_nhat & thoi_gian_yeu_cau
        return jsonify([_serialize_row(r) for r in rows]), 200

    except Error as e:
        return jsonify({"error": "DB_ERROR", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()
