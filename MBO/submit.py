# submit.py
from flask import Blueprint, request, jsonify
from database import get_connection
from flask_jwt_extended import jwt_required, get_jwt_identity

submit_bp = Blueprint('submit', __name__)

LEVEL_ORDER = [
    'group_name', 'section', 'sub_division', 'division', 'factory', 'company', 'corporation'
]

# -------------------------------
# Helper: require mbo_year từ request (2000..2100)
# Ưu tiên: ?mbo_year= | body.json["mbo_year"] | Header: X-MBO-Year
# -------------------------------
def _require_mbo_year_from_request():
    try:
        # 1) Query string
        val = request.args.get("mbo_year")
        if val is None:
            # 2) JSON body
            data = request.get_json(silent=True) or {}
            val = data.get("mbo_year")
        if val is None:
            # 3) Header
            val = request.headers.get("X-MBO-Year")

        year = int(val)
        if 2000 <= year <= 2100:
            return year
        return None
    except Exception:
        return None


# -------------------------------
@submit_bp.route('/mbo/submit', methods=['POST'])
def submit_mbo():
    data = request.json or {}
    employee_id = data.get('employee_id')
    if not employee_id:
        return jsonify({"error": "employee_id is required"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    conn = get_connection()
    try:
        # 1) Lấy thông tin nhân viên (buffered)
        with conn.cursor(dictionary=True, buffered=True) as cur:
            cur.execute("SELECT * FROM employees2026 WHERE id = %s", (employee_id,))
            emp = cur.fetchone()
        if not emp:
            return jsonify({"error": "employee not found"}), 404

        employee_code = emp['employee_code']

        # 2) Xác định cấp cao nhất có giá trị (không cần DB)
        highest_level = None
        for level in reversed(LEVEL_ORDER):
            if emp.get(level):
                highest_level = level
                break

        # 3) Kiểm tra có phải quản lý cấp cao nhất không (buffered)
        with conn.cursor(dictionary=True, buffered=True) as cur:
            cur.execute("""
                SELECT * FROM organization_units
                WHERE type = %s AND name = %s AND employee_id = %s
            """, (highest_level, emp[highest_level] if highest_level else None, employee_id))
            is_top_manager = cur.fetchone()

        if is_top_manager:
            reviewer_id = employee_id
            approver_id = employee_id
        else:
            # 4) Tìm reviewer (buffered mỗi lần)
            reviewer_unit = None
            for level in LEVEL_ORDER[1:]:
                unit_name = emp.get(level)
                if unit_name:
                    with conn.cursor(dictionary=True, buffered=True) as cur:
                        cur.execute(
                            "SELECT * FROM organization_units WHERE type = %s AND name = %s",
                            (level, unit_name)
                        )
                        unit = cur.fetchone()
                    if unit and unit.get('employee_id') and unit['employee_id'] != employee_id:
                        reviewer_unit = unit
                        break
            reviewer_id = reviewer_unit['employee_id'] if reviewer_unit else employee_id

            # 5) Tìm approver
            approver_id = None
            reviewer_level_index = LEVEL_ORDER.index(reviewer_unit['type']) if reviewer_unit else -1
            for i in range(reviewer_level_index + 1, len(LEVEL_ORDER)):
                upper_level = LEVEL_ORDER[i]
                upper_name = emp.get(upper_level)
                if upper_name:
                    with conn.cursor(dictionary=True, buffered=True) as cur:
                        cur.execute(
                            "SELECT * FROM organization_units WHERE type = %s AND name = %s",
                            (upper_level, upper_name)
                        )
                        unit = cur.fetchone()
                    if unit and unit.get('employee_id') and unit['employee_id'] != employee_id:
                        approver_id = unit['employee_id']
                        break

            if not approver_id:
                approver_id = reviewer_id

        # 6) Upsert vào bảng mbo_sessions
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO mbo_sessions (employee_id, mbo_year, status, reviewer_id, approver_id)
                VALUES (%s, %s, 'submitted', %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = 'submitted',
                    reviewer_id = VALUES(reviewer_id),
                    approver_id = VALUES(approver_id)
            """, (employee_id, mbo_year, reviewer_id, approver_id))
        conn.commit()

        auto_status = "submitted"

        # 7) Helpers — tất cả SELECT dùng buffered
        def auto_update_muctieu():
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute("""
                    SELECT id, ti_trong, xep_loai
                    FROM PersonalMBO
                    WHERE employee_code = %s AND mbo_year = %s
                """, (employee_code, mbo_year))
                goals = cur.fetchall()
            for g in goals:
                with conn.cursor() as cu:
                    cu.execute("""
                        UPDATE PersonalMBO SET
                            reviewer_ti_trong = %s,
                            reviewer_rating   = %s,
                            approver_ti_trong = %s,
                            approver_rating   = %s
                        WHERE id = %s AND mbo_year = %s
                    """, (g['ti_trong'], g['xep_loai'], g['ti_trong'], g['xep_loai'], g['id'], mbo_year))

        def auto_update_competency():
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute("""
                    SELECT id, ti_trong
                    FROM competencymbo
                    WHERE employee_code = %s AND mbo_year = %s
                """, (employee_code, mbo_year))
                goals = cur.fetchall()
            for g in goals:
                with conn.cursor() as cu:
                    cu.execute("""
                        UPDATE competencymbo SET
                            reviewer_ti_trong = %s,
                            approver_ti_trong = %s
                        WHERE id = %s AND mbo_year = %s
                    """, (g['ti_trong'], g['ti_trong'], g['id'], mbo_year))

        def approve_now():
            with conn.cursor() as c:
                c.execute("""
                    UPDATE mbo_sessions
                    SET status = 'approved'
                    WHERE employee_id = %s AND mbo_year = %s
                """, (employee_id, mbo_year))

        def review_now():
            with conn.cursor() as c:
                c.execute("""
                    UPDATE mbo_sessions
                    SET status = 'reviewed'
                    WHERE employee_id = %s AND mbo_year = %s
                """, (employee_id, mbo_year))

        # Case 1
        if reviewer_id == approver_id == employee_id:
            auto_update_muctieu()
            auto_update_competency()
            review_now()
            approve_now()
            auto_status = "approved"

        # Case 2
        elif reviewer_id == approver_id and reviewer_id != employee_id:
            # copy reviewer -> approver cho từng mục tiêu
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute("""
                    SELECT id FROM PersonalMBO
                    WHERE employee_code = %s AND mbo_year = %s
                """, (employee_code, mbo_year))
                pgoals = cur.fetchall()
            for g in pgoals:
                with conn.cursor() as cu:
                    cu.execute("""
                        UPDATE PersonalMBO SET
                            approver_ti_trong = reviewer_ti_trong,
                            approver_rating   = reviewer_rating
                        WHERE id = %s AND mbo_year = %s
                    """, (g['id'], mbo_year))

            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute("""
                    SELECT id FROM competencymbo
                    WHERE employee_code = %s AND mbo_year = %s
                """, (employee_code, mbo_year))
                cgoals = cur.fetchall()
            for g in cgoals:
                with conn.cursor() as cu:
                    cu.execute("""
                        UPDATE competencymbo SET
                            approver_ti_trong = reviewer_ti_trong
                        WHERE id = %s AND mbo_year = %s
                    """, (g['id'], mbo_year))

            review_now()
            auto_status = "reviewed"

        conn.commit()

        return jsonify({
            "success": True,
            "reviewer_id": reviewer_id,
            "approver_id": approver_id,
            "status": auto_status,
            "mbo_year": mbo_year
        })

    except Exception as e:
        conn.rollback()
        print("❌ submit_mbo error:", e)
        return jsonify({"error": "submit_mbo failed", "detail": str(e)}), 500
    finally:
        conn.close()


# -------------------------------
# GET SESSION STATUS
# -------------------------------
@submit_bp.route('/mbo/session-status/<int:employee_id>', methods=['GET'])
def get_mbo_status(employee_id):
    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"status": "draft", "note": "Thiếu mbo_year"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT status FROM mbo_sessions
        WHERE employee_id = %s AND mbo_year = %s
    """, (employee_id, mbo_year))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return jsonify({"status": row['status'], "mbo_year": mbo_year})
    else:
        return jsonify({"status": "draft", "mbo_year": mbo_year})


# -------------------------------
# CHECK PERMISSIONS
# -------------------------------
@submit_bp.route('/mbo/permissions/<int:employee_id>', methods=['GET'])
@jwt_required()
def check_mbo_permissions(employee_id):
    current_user_id = get_jwt_identity()
    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT reviewer_id, approver_id, status
        FROM mbo_sessions
        WHERE employee_id = %s AND mbo_year = %s
    """, (employee_id, mbo_year))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return jsonify({
            "canReview": False,
            "canApprove": False,
            "mbo_year": mbo_year
        })

    status = row['status']
    can_review = (row['reviewer_id'] == current_user_id) and status == 'submitted'
    can_approve = (row['approver_id'] == current_user_id) and status == 'reviewed'

    return jsonify({
        "canReview": can_review,
        "canApprove": can_approve,
        "mbo_year": mbo_year
    })


# -------------------------------
# REVIEW
# -------------------------------
@submit_bp.route('/mbo/review', methods=['POST'])
def review_mbo():
    data = request.json or {}
    employee_id = data.get("employee_id")
    if not employee_id:
        return jsonify({"error": "Thiếu employee_id"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = """
                UPDATE mbo_sessions
                SET status = %s
                WHERE employee_id = %s AND mbo_year = %s
            """
            cursor.execute(sql, ('reviewed', employee_id, mbo_year))

        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"error": "Không tìm thấy session để cập nhật"}), 404

        return jsonify({"message": "Cập nhật trạng thái reviewed thành công.", "mbo_year": mbo_year}), 200

    except Exception as e:
        print("❌ Lỗi khi cập nhật trạng thái reviewed:", e)
        return jsonify({"error": "Đã có lỗi xảy ra."}), 500
    finally:
        try:
            conn.close()
        except:
            pass


@submit_bp.route('/mbo/approve', methods=['POST'])
def approve_mbo():
    data = request.json or {}
    employee_id = data.get("employee_id")
    if not employee_id:
        return jsonify({"error": "Thiếu employee_id"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = """
                UPDATE mbo_sessions
                SET status = %s
                WHERE employee_id = %s AND mbo_year = %s
            """
            cursor.execute(sql, ('approved', employee_id, mbo_year))

        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"error": "Không tìm thấy session để cập nhật"}), 404

        return jsonify({"message": "Cập nhật trạng thái approved thành công.", "mbo_year": mbo_year}), 200

    except Exception as e:
        print("❌ Lỗi khi cập nhật trạng thái approved:", e)
        return jsonify({"error": "Đã có lỗi xảy ra."}), 500
    finally:
        try:
            conn.close()
        except:
            pass


# -------------------------------
# Helper: tính reviewer/approver theo cây tổ chức
# -------------------------------
def _calc_reviewer_approver(conn, emp_row):
    cursor = conn.cursor(dictionary=True)

    employee_id = emp_row['id']

    # 1) Xác định cấp cao nhất có giá trị
    highest_level = None
    for level in reversed(LEVEL_ORDER):
        if emp_row.get(level):
            highest_level = level
            break

    # 2) Nếu chính nhân sự là quản lý cấp cao nhất -> tự reviewer/approver
    cursor.execute("""
        SELECT * FROM organization_units
        WHERE type = %s AND name = %s AND employee_id = %s
    """, (highest_level, emp_row[highest_level] if highest_level else None, employee_id))
    is_top_manager = cursor.fetchone()

    if is_top_manager:
        cursor.close()
        return employee_id, employee_id

    # 3) Tìm reviewer (cấp cao hơn gần nhất, khác chính mình)
    reviewer_unit = None
    for level in LEVEL_ORDER[1:]:
        unit_name = emp_row.get(level)
        if not unit_name:
            continue
        cursor.execute(
            "SELECT * FROM organization_units WHERE type = %s AND name = %s",
            (level, unit_name)
        )
        unit = cursor.fetchone()
        if unit and unit.get('employee_id') and unit['employee_id'] != employee_id:
            reviewer_unit = unit
            break

    reviewer_id = reviewer_unit['employee_id'] if reviewer_unit else employee_id

    # 4) Tìm approver (cấp cao hơn reviewer)
    approver_id = None
    reviewer_level_index = LEVEL_ORDER.index(reviewer_unit['type']) if reviewer_unit else -1
    for i in range(reviewer_level_index + 1, len(LEVEL_ORDER)):
        upper_level = LEVEL_ORDER[i]
        upper_name = emp_row.get(upper_level)
        if not upper_name:
            continue
        cursor.execute(
            "SELECT * FROM organization_units WHERE type = %s AND name = %s",
            (upper_level, upper_name)
        )
        unit = cursor.fetchone()
        if unit and unit.get('employee_id') and unit['employee_id'] != employee_id:
            approver_id = unit['employee_id']
            break

    if not approver_id:
        approver_id = reviewer_id

    cursor.close()
    return reviewer_id, approver_id


# -------------------------------
# SUBMIT FINAL (tự đánh giá cuối năm)
# -------------------------------

# --- Helpers cập nhật trạng thái FINAL ---
def _update_mbo_status_final(conn, employee_id: int, mbo_year: int, new_status: str):
    with conn.cursor() as c:
        c.execute(
            """
            UPDATE mbo_sessions
            SET status = %s
            WHERE employee_id = %s AND mbo_year = %s
            """,
            (new_status, employee_id, mbo_year),
        )

def _review_final_now(conn, employee_id: int, mbo_year: int):
    _update_mbo_status_final(conn, employee_id, mbo_year, "reviewed_final")

def _approve_final_now(conn, employee_id: int, mbo_year: int):
    _update_mbo_status_final(conn, employee_id, mbo_year, "approved_final")


# ===========================
#   CÁC HELPER ĐÃ SỬA AN TOÀN
# ===========================

def _auto_update_personal_mbo_copy_self_to_review_and_approve(conn, employee_code: str, mbo_year: int):
    """
    Case 1: người lập = reviewer = approver
    Copy self (ti_trong, xep_loai) -> reviewer_* và approver_*,
    nhưng CHỈ ghi đè khi nguồn có giá trị; nếu nguồn NULL thì giữ nguyên.
    """
    with conn.cursor() as c:
        c.execute(
            """
            UPDATE PersonalMBO
            SET
              reviewer_ti_trong = IF(ti_trong IS NOT NULL, ti_trong, reviewer_ti_trong),
              reviewer_rating   = IF(xep_loai IS NOT NULL,  xep_loai, reviewer_rating),
              approver_ti_trong = IF(ti_trong IS NOT NULL, ti_trong, approver_ti_trong),
              approver_rating   = IF(xep_loai IS NOT NULL,  xep_loai, approver_rating)
            WHERE employee_code = %s AND mbo_year = %s
            """,
            (employee_code, mbo_year),
        )

def _auto_update_competency_copy_self_to_review_and_approve(conn, employee_code: str, mbo_year: int):
    """
    Case 1: người lập = reviewer = approver
    Copy self (ti_trong) -> reviewer_/approver_ ti_trong cho competencymbo,
    có guard tránh ghi NULL.
    """
    with conn.cursor() as c:
        c.execute(
            """
            UPDATE competencymbo
            SET
              reviewer_ti_trong = IF(ti_trong IS NOT NULL, ti_trong, reviewer_ti_trong),
              approver_ti_trong = IF(ti_trong IS NOT NULL, ti_trong, approver_ti_trong)
            WHERE employee_code = %s AND mbo_year = %s
            """,
            (employee_code, mbo_year),
        )

def _auto_copy_reviewer_to_approver(conn, employee_code: str, mbo_year: int):
    """
    Case 2: reviewer = approver ≠ người lập
    Copy reviewer_* -> approver_* cho cả PersonalMBO và competencymbo,
    chỉ ghi đè khi reviewer_* có giá trị.
    """
    with conn.cursor() as c:
        # PersonalMBO
        c.execute(
            """
            UPDATE PersonalMBO
            SET
              approver_ti_trong = IF(reviewer_ti_trong IS NOT NULL, reviewer_ti_trong, approver_ti_trong),
              approver_rating   = IF(reviewer_rating   IS NOT NULL, reviewer_rating,   approver_rating)
            WHERE employee_code = %s AND mbo_year = %s
            """,
            (employee_code, mbo_year),
        )
        # competencymbo
        c.execute(
            """
            UPDATE competencymbo
            SET
              approver_ti_trong = IF(reviewer_ti_trong IS NOT NULL, reviewer_ti_trong, approver_ti_trong)
            WHERE employee_code = %s AND mbo_year = %s
            """,
            (employee_code, mbo_year),
        )


@submit_bp.route('/mbo/submit-final', methods=['POST'])
def submit_mbo_final():
    """
    Gửi tự đánh giá cuối năm:
      - Mặc định: status = 'submitted_final'
      - Case 1: người lập = reviewer = approver  -> auto reviewed_final + approved_final, và copy self -> reviewer/approver
      - Case 2: reviewer = approver ≠ người lập  -> copy reviewer -> approver, set reviewed_final
    Body JSON: { "employee_id": 123, "mbo_year": 2025 } hoặc ?mbo_year=2025
    """
    data = request.json or {}
    employee_id = data.get('employee_id')
    if not employee_id:
        return jsonify({"error": "employee_id is required"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # 1) Lấy thông tin nhân viên
        cursor.execute("SELECT * FROM employees2026 WHERE id = %s", (employee_id,))
        emp = cursor.fetchone()
        if not emp:
            return jsonify({"error": "employee not found"}), 404

        employee_code = emp["employee_code"]

        # 2) Tính reviewer/approver cho giai đoạn cuối năm
        reviewer_id, approver_id = _calc_reviewer_approver(conn, emp)

        # 3) Upsert session về submitted_final + set reviewer/approver
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO mbo_sessions (employee_id, mbo_year, status, reviewer_id, approver_id)
                VALUES (%s, %s, 'submitted_final', %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = 'submitted_final',
                    reviewer_id = VALUES(reviewer_id),
                    approver_id = VALUES(approver_id)
                """,
                (employee_id, mbo_year, reviewer_id, approver_id),
            )

        auto_status = "submitted_final"

        # 4) Auto chuyển FINAL theo rule
        # Case 1: người lập = reviewer = approver
        if reviewer_id == approver_id == employee_id:
            _auto_update_personal_mbo_copy_self_to_review_and_approve(conn, employee_code, mbo_year)
            _auto_update_competency_copy_self_to_review_and_approve(conn, employee_code, mbo_year)
            _review_final_now(conn, employee_id, mbo_year)
            _approve_final_now(conn, employee_id, mbo_year)
            auto_status = "approved_final"

        # Case 2: reviewer = approver ≠ người lập
        elif reviewer_id == approver_id and reviewer_id != employee_id:
            _auto_copy_reviewer_to_approver(conn, employee_code, mbo_year)
            _review_final_now(conn, employee_id, mbo_year)
            auto_status = "reviewed_final"

        # (Không rơi vào Case 1/2) -> giữ submitted_final, KHÔNG động vào dữ liệu mục tiêu

        conn.commit()

        return jsonify({
            "success": True,
            "reviewer_id": reviewer_id,
            "approver_id": approver_id,
            "status": auto_status,
            "mbo_year": mbo_year
        }), 200

    except Exception as e:
        conn.rollback()
        print("❌ submit_mbo_final error:", e)
        return jsonify({"error": "submit_mbo_final failed", "detail": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@submit_bp.route('/mbo/reviewed_final', methods=['POST'])
def reviewed_final_mbo():
    data = request.json or {}
    employee_id = data.get("employee_id")
    if not employee_id:
        return jsonify({"error": "Thiếu employee_id"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    try:
        conn = get_connection()
        _review_final_now(conn, employee_id, mbo_year)
        conn.commit()
        return jsonify({"message": "Cập nhật trạng thái reviewed_final thành công.", "mbo_year": mbo_year}), 200
    except Exception as e:
        print("❌ reviewed_final error:", e)
        return jsonify({"error": "Đã có lỗi xảy ra."}), 500
    finally:
        try:
            conn.close()
        except:
            pass


@submit_bp.route('/mbo/approved_final', methods=['POST'])
def approved_final_mbo():
    data = request.json or {}
    employee_id = data.get("employee_id")
    if not employee_id:
        return jsonify({"error": "Thiếu employee_id"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    try:
        conn = get_connection()
        _approve_final_now(conn, employee_id, mbo_year)
        conn.commit()
        return jsonify({"message": "Cập nhật trạng thái approved_final thành công.", "mbo_year": mbo_year}), 200
    except Exception as e:
        print("❌ approved_final error:", e)
        return jsonify({"error": "Đã có lỗi xảy ra."}), 500
    finally:
        try:
            conn.close()
        except:
            pass


def _update_mbo_status(employee_id: int, mbo_year: int, new_status: str):
    """Cập nhật trạng thái MBO cho employee_id + mbo_year vào giá trị new_status."""
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = """
                UPDATE mbo_sessions
                SET status = %s
                WHERE employee_id = %s AND mbo_year = %s
            """
            cursor.execute(sql, (new_status, employee_id, mbo_year))
        conn.commit()

        if cursor.rowcount == 0:
            return {"error": "Không tìm thấy session để cập nhật"}, 404

        return {"message": f"Cập nhật trạng thái {new_status} thành công.", "mbo_year": mbo_year}, 200

    except Exception as e:
        print(f"❌ Lỗi khi cập nhật trạng thái {new_status}:", e)
        return {"error": "Đã có lỗi xảy ra."}, 500
    finally:
        try:
            conn.close()
        except:
            pass
