# submit.py
from flask import Blueprint, request, jsonify
from database import get_connection
from flask_jwt_extended import jwt_required, get_jwt_identity

submit_bp = Blueprint("submit", __name__)

LEVEL_ORDER = [
    "group_name",
    "section",
    "sub_division",
    "division",
    "factory",
    "company",
    "corporation",
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
@submit_bp.route("/mbo/submit", methods=["POST"])
def submit_mbo():
    data = request.json or {}
    employee_id = data.get("employee_id")
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

        employee_code = emp["employee_code"]
        leaf_unit_id = emp.get("organization_unit_id")

        # ===== Helpers: truy đơn vị và leo lên cha =====
        def get_unit_by_id(uid):
            if not uid:
                return None
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute(
                    """
                    SELECT id, name, type, parent_id, employee_id
                    FROM organization_units
                    WHERE id = %s
                """,
                    (uid,),
                )
                return cur.fetchone()

        def climb_chain_from(start_unit_id, max_depth=64):
            """Trả về list [leaf, ..., root]. Có chống vòng lặp."""
            chain, seen, u = [], set(), get_unit_by_id(start_unit_id)
            while u and u["id"] not in seen and len(chain) < max_depth:
                chain.append(u)
                seen.add(u["id"])
                if not u.get("parent_id"):
                    break
                u = get_unit_by_id(u["parent_id"])
            return chain  # chain[0] = leaf

        # ===== Rule position hợp lệ cho Reviewer (>= Trưởng phòng) =====
        REVIEWER_OK_POSITIONS = {
            "tổng giám đốc",
            "phó tổng giám đốc",
            "giám đốc",
            "phó giám đốc",
            "trưởng phòng cấp cao",
            "phó phòng cấp cao",
            "trưởng phòng",
        }

        def get_employee_position(eid):
            if not eid:
                return None
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute(
                    """
                    SELECT position
                    FROM nsh.employees2026_base
                    WHERE id = %s
                    LIMIT 1
                """,
                    (eid,),
                )
                row = cur.fetchone()
            return row["position"] if row else None

        # ===== Tìm reviewer/approver theo rule mới (bỏ LEVEL_ORDER) =====
        reviewer_id = employee_id
        approver_id = employee_id
        reviewer_unit = None

        if leaf_unit_id:
            path = climb_chain_from(leaf_unit_id)  # từ đơn vị NV -> đỉnh

            # --- Reviewer:
            # Chỉ chọn manager khác NV và position phải từ Trưởng phòng trở lên.
            if path:
                for u in path:  # từ đơn vị hiện tại lên dần
                    mid = u.get("employee_id")
                    if not mid:
                        continue
                    if mid == employee_id:
                        continue

                    pos = get_employee_position(mid)
                    pos_norm = (pos or "").strip().lower()

                    if pos_norm in REVIEWER_OK_POSITIONS:
                        reviewer_unit = u
                        break
            reviewer_id = reviewer_unit["employee_id"] if reviewer_unit else employee_id

            # --- Approver (cấp NGAY TRÊN reviewer):
            approver_unit = None
            if reviewer_unit:
                # Tìm vị trí reviewer trong path [leaf,...,root]
                try:
                    idx = next(
                        i for i, u in enumerate(path) if u["id"] == reviewer_unit["id"]
                    )
                except StopIteration:
                    idx = -1

                if idx >= 0 and idx + 1 < len(path):
                    upper = path[idx + 1]
                    upper_mid = upper.get("employee_id")

                    if upper_mid:
                        if upper_mid == reviewer_id:
                            approver_unit = upper
                            approver_id = reviewer_id
                        elif upper_mid != employee_id:
                            approver_unit = upper
                            approver_id = upper_mid
                        else:
                            for u in path[idx + 2 :]:
                                mid = u.get("employee_id")
                                if mid and mid != employee_id and mid != reviewer_id:
                                    approver_unit = u
                                    approver_id = mid
                                    break
                            if not approver_unit:
                                approver_id = reviewer_id
                    else:
                        for u in path[idx + 2 :]:
                            mid = u.get("employee_id")
                            if mid and mid != employee_id and mid != reviewer_id:
                                approver_unit = u
                                approver_id = mid
                                break
                        if not approver_unit:
                            approver_id = reviewer_id
                else:
                    approver_id = reviewer_id
            else:
                for u in path[1:]:
                    mid = u.get("employee_id")
                    if mid and mid != employee_id:
                        approver_unit = u
                        approver_id = mid
                        break
                if not approver_unit:
                    approver_id = reviewer_id
        # Nếu không có organization_unit_id: giữ mặc định self/self

        # 6) Upsert vào bảng mbo_sessions
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO mbo_sessions (employee_id, mbo_year, status, reviewer_id, approver_id)
                VALUES (%s, %s, 'submitted', %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = 'submitted',
                    reviewer_id = VALUES(reviewer_id),
                    approver_id = VALUES(approver_id)
            """,
                (employee_id, mbo_year, reviewer_id, approver_id),
            )
        conn.commit()

        auto_status = "submitted"

        # 7) Helpers — tất cả SELECT dùng buffered
        def auto_update_muctieu():
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute(
                    """
                    SELECT id, ti_trong, xep_loai
                    FROM PersonalMBO
                    WHERE employee_code = %s AND mbo_year = %s
                """,
                    (employee_code, mbo_year),
                )
                goals = cur.fetchall()
            for g in goals:
                with conn.cursor() as cu:
                    cu.execute(
                        """
                        UPDATE PersonalMBO SET
                            reviewer_ti_trong = %s,
                            reviewer_rating   = %s,
                            approver_ti_trong = %s,
                            approver_rating   = %s
                        WHERE id = %s AND mbo_year = %s
                    """,
                        (
                            g["ti_trong"],
                            g["xep_loai"],
                            g["ti_trong"],
                            g["xep_loai"],
                            g["id"],
                            mbo_year,
                        ),
                    )

        def auto_update_competency():
            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute(
                    """
                    SELECT id, ti_trong
                    FROM competencymbo
                    WHERE employee_code = %s AND mbo_year = %s
                """,
                    (employee_code, mbo_year),
                )
                goals = cur.fetchall()
            for g in goals:
                with conn.cursor() as cu:
                    cu.execute(
                        """
                        UPDATE competencymbo SET
                            reviewer_ti_trong = %s,
                            approver_ti_trong = %s
                        WHERE id = %s AND mbo_year = %s
                    """,
                        (g["ti_trong"], g["ti_trong"], g["id"], mbo_year),
                    )

        def approve_now():
            with conn.cursor() as c:
                c.execute(
                    """
                    UPDATE mbo_sessions
                    SET status = 'approved'
                    WHERE employee_id = %s AND mbo_year = %s
                """,
                    (employee_id, mbo_year),
                )

        def review_now():
            with conn.cursor() as c:
                c.execute(
                    """
                    UPDATE mbo_sessions
                    SET status = 'reviewed'
                    WHERE employee_id = %s AND mbo_year = %s
                """,
                    (employee_id, mbo_year),
                )

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
                cur.execute(
                    """
                    SELECT id FROM PersonalMBO
                    WHERE employee_code = %s AND mbo_year = %s
                """,
                    (employee_code, mbo_year),
                )
                pgoals = cur.fetchall()
            for g in pgoals:
                with conn.cursor() as cu:
                    cu.execute(
                        """
                        UPDATE PersonalMBO SET
                            approver_ti_trong = reviewer_ti_trong,
                            approver_rating   = reviewer_rating
                        WHERE id = %s AND mbo_year = %s
                    """,
                        (g["id"], mbo_year),
                    )

            with conn.cursor(dictionary=True, buffered=True) as cur:
                cur.execute(
                    """
                    SELECT id FROM competencymbo
                    WHERE employee_code = %s AND mbo_year = %s
                """,
                    (employee_code, mbo_year),
                )
                cgoals = cur.fetchall()
            for g in cgoals:
                with conn.cursor() as cu:
                    cu.execute(
                        """
                        UPDATE competencymbo SET
                            approver_ti_trong = reviewer_ti_trong
                        WHERE id = %s AND mbo_year = %s
                    """,
                        (g["id"], mbo_year),
                    )

            review_now()
            auto_status = "reviewed"

        conn.commit()

        return jsonify(
            {
                "success": True,
                "reviewer_id": reviewer_id,
                "approver_id": approver_id,
                "status": auto_status,
                "mbo_year": mbo_year,
            }
        )

    except Exception as e:
        conn.rollback()
        print("❌ submit_mbo error:", e)
        return jsonify({"error": "submit_mbo failed", "detail": str(e)}), 500
    finally:
        conn.close()


# -------------------------------
# GET SESSION STATUS
# -------------------------------
@submit_bp.route("/mbo/session-status/<int:employee_id>", methods=["GET"])
def get_mbo_status(employee_id):
    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"status": "draft", "note": "Thiếu mbo_year"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT status FROM mbo_sessions
        WHERE employee_id = %s AND mbo_year = %s
    """,
        (employee_id, mbo_year),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return jsonify({"status": row["status"], "mbo_year": mbo_year})
    else:
        return jsonify({"status": "draft", "mbo_year": mbo_year})


# -------------------------------
# CHECK PERMISSIONS
# -------------------------------
@submit_bp.route("/mbo/permissions/<int:employee_id>", methods=["GET"])
@jwt_required()
def check_mbo_permissions(employee_id):
    current_user_id = get_jwt_identity()
    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT reviewer_id, approver_id, status
        FROM mbo_sessions
        WHERE employee_id = %s AND mbo_year = %s
    """,
        (employee_id, mbo_year),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return jsonify({"canReview": False, "canApprove": False, "mbo_year": mbo_year})

    status = row["status"]
    can_review = (row["reviewer_id"] == current_user_id) and status == "submitted"
    can_approve = (row["approver_id"] == current_user_id) and status == "reviewed"

    return jsonify({"canReview": can_review, "canApprove": can_approve, "mbo_year": mbo_year})


# -------------------------------
# REVIEW
# -------------------------------
@submit_bp.route("/mbo/review", methods=["POST"])
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
            cursor.execute(sql, ("reviewed", employee_id, mbo_year))
            rowcount = cursor.rowcount  # tránh phụ thuộc cursor sau when-exit

        conn.commit()

        if rowcount == 0:
            return jsonify({"error": "Không tìm thấy session để cập nhật"}), 404

        return jsonify({"message": "Cập nhật trạng thái reviewed thành công.", "mbo_year": mbo_year}), 200

    except Exception as e:
        print("❌ Lỗi khi cập nhật trạng thái reviewed:", e)
        return jsonify({"error": "Đã có lỗi xảy ra."}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@submit_bp.route("/mbo/approve", methods=["POST"])
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
            cursor.execute(sql, ("approved", employee_id, mbo_year))
            rowcount = cursor.rowcount

        conn.commit()

        if rowcount == 0:
            return jsonify({"error": "Không tìm thấy session để cập nhật"}), 404

        return jsonify({"message": "Cập nhật trạng thái approved thành công.", "mbo_year": mbo_year}), 200

    except Exception as e:
        print("❌ Lỗi khi cập nhật trạng thái approved:", e)
        return jsonify({"error": "Đã có lỗi xảy ra."}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ============================================================
# FIX LỖI: submit-final KHÔNG dùng (type,name) nữa để tránh trùng
# -> Tính reviewer/approver theo organization_unit_id (ID + parent_id)
#    giống /mbo/submit => không còn Unread result found, không chọn nhầm nhánh
# ============================================================
def _calc_reviewer_approver_final_by_unit_tree(conn, employee_id: int, leaf_unit_id: int):
    """
    Tính reviewer/approver cho FINAL theo cây organization_units bằng ID (an toàn khi name/type trùng).
    - Reviewer: manager khác NV và position thuộc nhóm >= Trưởng phòng.
    - Approver: cấp ngay trên reviewer trong path; fallback leo tiếp tìm manager khác.
    """
    # Rule position hợp lệ cho Reviewer (>= Trưởng phòng)
    REVIEWER_OK_POSITIONS = {
        "tổng giám đốc",
        "phó tổng giám đốc",
        "giám đốc",
        "phó giám đốc",
        "trưởng phòng cấp cao",
        "phó phòng cấp cao",
        "trưởng phòng",
    }

    def get_employee_position(eid):
        if not eid:
            return None
        with conn.cursor(dictionary=True, buffered=True) as cur:
            cur.execute(
                """
                SELECT position
                FROM nsh.employees2026_base
                WHERE id = %s
                LIMIT 1
                """,
                (eid,),
            )
            row = cur.fetchone()
        return row["position"] if row else None

    def get_unit_by_id(uid):
        if not uid:
            return None
        with conn.cursor(dictionary=True, buffered=True) as cur:
            cur.execute(
                """
                SELECT id, name, type, parent_id, employee_id
                FROM organization_units
                WHERE id = %s
                LIMIT 1
                """,
                (uid,),
            )
            return cur.fetchone()

    def climb_chain_from(start_unit_id, max_depth=64):
        chain, seen = [], set()
        u = get_unit_by_id(start_unit_id)
        while u and u["id"] not in seen and len(chain) < max_depth:
            chain.append(u)  # leaf -> root
            seen.add(u["id"])
            pid = u.get("parent_id")
            if not pid:
                break
            u = get_unit_by_id(pid)
        return chain

    reviewer_id = employee_id
    approver_id = employee_id
    reviewer_unit = None

    if not leaf_unit_id:
        return reviewer_id, approver_id

    path = climb_chain_from(leaf_unit_id)

    # reviewer: manager khác NV và position hợp lệ
    for u in path:
        mid = u.get("employee_id")
        if not mid or mid == employee_id:
            continue
        pos_norm = (get_employee_position(mid) or "").strip().lower()
        if pos_norm in REVIEWER_OK_POSITIONS:
            reviewer_unit = u
            reviewer_id = mid
            break

    # approver: cấp ngay trên reviewer trong path; fallback leo tiếp
    if reviewer_unit:
        idx = next((i for i, u in enumerate(path) if u["id"] == reviewer_unit["id"]), -1)
        if idx >= 0 and idx + 1 < len(path):
            upper = path[idx + 1]
            upper_mid = upper.get("employee_id")
            if upper_mid and upper_mid != employee_id:
                # nếu upper_mid == reviewer_id -> approver = reviewer (KHÔNG leo tiếp)
                approver_id = reviewer_id if upper_mid == reviewer_id else upper_mid
            else:
                # leo tiếp tìm manager khác reviewer & khác NV
                found = None
                for uu in path[idx + 2 :]:
                    mid = uu.get("employee_id")
                    if mid and mid not in (employee_id, reviewer_id):
                        found = mid
                        break
                approver_id = found if found else reviewer_id
        else:
            approver_id = reviewer_id
    else:
        # reviewer = self -> approver = manager đầu tiên phía trên (nếu có)
        found = None
        for u in path[1:]:
            mid = u.get("employee_id")
            if mid and mid != employee_id:
                found = mid
                break
        approver_id = found if found else reviewer_id

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


@submit_bp.route("/mbo/submit-final", methods=["POST"])
def submit_mbo_final():
    """
    Gửi tự đánh giá cuối năm:
      - Mặc định: status = 'submitted_final'
      - Case 1: người lập = reviewer = approver  -> auto reviewed_final + approved_final, và copy self -> reviewer/approver
      - Case 2: reviewer = approver ≠ người lập  -> copy reviewer -> approver, set reviewed_final
    Body JSON: { "employee_id": 123, "mbo_year": 2025 } hoặc ?mbo_year=2025
    """
    data = request.json or {}
    employee_id = data.get("employee_id")
    if not employee_id:
        return jsonify({"error": "employee_id is required"}), 400

    mbo_year = _require_mbo_year_from_request()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    conn = get_connection()
    # ✅ buffered=True để tránh Unread result found trong mọi trường hợp
    cursor = conn.cursor(dictionary=True, buffered=True)

    try:
        # 1) Lấy thông tin nhân viên
        cursor.execute("SELECT * FROM employees2026 WHERE id = %s", (employee_id,))
        emp = cursor.fetchone()
        if not emp:
            return jsonify({"error": "employee not found"}), 404

        employee_code = emp["employee_code"]
        leaf_unit_id = emp.get("organization_unit_id")

        # 2) Tính reviewer/approver cho giai đoạn cuối năm (FIX: theo organization_unit_id, không theo type+name)
        reviewer_id, approver_id = _calc_reviewer_approver_final_by_unit_tree(
            conn, employee_id, leaf_unit_id
        )

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

        return (
            jsonify(
                {
                    "success": True,
                    "reviewer_id": reviewer_id,
                    "approver_id": approver_id,
                    "status": auto_status,
                    "mbo_year": mbo_year,
                }
            ),
            200,
        )

    except Exception as e:
        conn.rollback()
        print("❌ submit_mbo_final error:", e)
        return jsonify({"error": "submit_mbo_final failed", "detail": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@submit_bp.route("/mbo/reviewed_final", methods=["POST"])
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
        except Exception:
            pass


@submit_bp.route("/mbo/approved_final", methods=["POST"])
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
        except Exception:
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
            rowcount = cursor.rowcount

        conn.commit()

        if rowcount == 0:
            return {"error": "Không tìm thấy session để cập nhật"}, 404

        return {"message": f"Cập nhật trạng thái {new_status} thành công.", "mbo_year": mbo_year}, 200

    except Exception as e:
        print(f"❌ Lỗi khi cập nhật trạng thái {new_status}:", e)
        return {"error": "Đã có lỗi xảy ra."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass
