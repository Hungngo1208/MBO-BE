from flask import Blueprint, request, jsonify
from database import get_connection

# Blueprint
allocations_bp = Blueprint("allocations", __name__)

# ======================
# Common validators
# ======================
def _require_mbo_year(payload):
    year = payload.get("mbo_year")
    try:
        year = int(year)
    except (TypeError, ValueError):
        return None
    if year < 2000 or year > 2100:
        return None
    return year

# ======================
# Schema helpers
# ======================
def _table_has_column(cursor, table, column):
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        LIMIT 1
    """,
        (table, column),
    )
    return cursor.fetchone() is not None


def _has_receiver_goal_id(cursor):
    cursor.execute("SHOW COLUMNS FROM mbo_allocations LIKE 'receiver_goal_id'")
    return cursor.fetchone() is not None


def _table_has_table(cursor, schema_table):
    """
    Kiểm tra sự tồn tại của bảng (hỗ trợ cả dạng 'nsh.personalmbo' hoặc chỉ 'personalmbo').
    """
    if "." in schema_table:
        schema, table = schema_table.split(".", 1)
        cursor.execute(
            """
            SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            LIMIT 1
            """,
            (schema, table),
        )
    else:
        table = schema_table
        cursor.execute(
            """
            SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME=%s
            LIMIT 1
            """,
            (table,),
        )
    return cursor.fetchone() is not None


# ======================
# Domain helpers
# ======================

def _fetch_sender_goal(cursor, goal_id, sender_code, mbo_year):
    """
    Lấy record nguồn từ personalmbo theo id + employee_code (+ mbo_year nếu có cột).
    """
    has_year = _table_has_column(cursor, "personalmbo", "mbo_year")
    select_sql = f"""
        SELECT id, employee_code{', mbo_year' if has_year else ''},
               ten_muc_tieu, mo_ta, don_vi_do_luong,
               gia_tri_ban_dau, muc_tieu, han_hoan_thanh,
               created_at, updated_at
        FROM personalmbo
        WHERE id = %s AND employee_code = %s
        {"AND mbo_year = %s" if has_year else ""}
        LIMIT 1
    """
    params = [goal_id, sender_code]
    if has_year:
        params.append(mbo_year)
    cursor.execute(select_sql, params)
    return cursor.fetchone()


def _insert_receiver_goal(cursor, receiver_code, mbo_year, src_goal, allocation_value):
    """
    Copy goal sang người nhận:
    - muc_tieu = allocation_value (giá trị phân bổ, KIỂU CHUỖI)
    - nếu có cột phan_loai => set 'nhan'
    - nếu có cột mbo_year => chèn năm
    """
    has_year = _table_has_column(cursor, "personalmbo", "mbo_year")
    has_phan_loai = _table_has_column(cursor, "personalmbo", "phan_loai")

    if has_year and has_phan_loai:
        cursor.execute(
            """
            INSERT INTO personalmbo
                (employee_code, mbo_year,
                 ten_muc_tieu, mo_ta, don_vi_do_luong,
                 gia_tri_ban_dau, muc_tieu, han_hoan_thanh,
                 phan_loai,
                 created_at, updated_at)
            VALUES
                (%s, %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 'nhan',
                 NOW(), NOW())
        """,
            (
                receiver_code,
                mbo_year,
                src_goal["ten_muc_tieu"],
                src_goal["mo_ta"],
                src_goal["don_vi_do_luong"],
                src_goal["gia_tri_ban_dau"],
                str(allocation_value),  # giữ chuỗi
                src_goal["han_hoan_thanh"],
            ),
        )
    elif has_year and not has_phan_loai:
        cursor.execute(
            """
            INSERT INTO personalmbo
                (employee_code, mbo_year,
                 ten_muc_tieu, mo_ta, don_vi_do_luong,
                 gia_tri_ban_dau, muc_tieu, han_hoan_thanh,
                 created_at, updated_at)
            VALUES
                (%s, %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 NOW(), NOW())
        """,
            (
                receiver_code,
                mbo_year,
                src_goal["ten_muc_tieu"],
                src_goal["mo_ta"],
                src_goal["don_vi_do_luong"],
                src_goal["gia_tri_ban_dau"],
                str(allocation_value),
                src_goal["han_hoan_thanh"],
            ),
        )
    elif not has_year and has_phan_loai:
        cursor.execute(
            """
            INSERT INTO personalmbo
                (employee_code,
                 ten_muc_tieu, mo_ta, don_vi_do_luong,
                 gia_tri_ban_dau, muc_tieu, han_hoan_thanh,
                 phan_loai,
                 created_at, updated_at)
            VALUES
                (%s,
                 %s, %s, %s,
                 %s, %s, %s,
                 'nhan',
                 NOW(), NOW())
        """,
            (
                receiver_code,
                src_goal["ten_muc_tieu"],
                src_goal["mo_ta"],
                src_goal["don_vi_do_luong"],
                src_goal["gia_tri_ban_dau"],
                str(allocation_value),
                src_goal["han_hoan_thanh"],
            ),
        )
    else:
        cursor.execute(
            """
            INSERT INTO personalmbo
                (employee_code,
                 ten_muc_tieu, mo_ta, don_vi_do_luong,
                 gia_tri_ban_dau, muc_tieu, han_hoan_thanh,
                 created_at, updated_at)
            VALUES
                (%s,
                 %s, %s, %s,
                 %s, %s, %s,
                 NOW(), NOW())
        """,
            (
                receiver_code,
                src_goal["ten_muc_tieu"],
                src_goal["mo_ta"],
                src_goal["don_vi_do_luong"],
                src_goal["gia_tri_ban_dau"],
                str(allocation_value),
                src_goal["han_hoan_thanh"],
            ),
        )

    return cursor.lastrowid


def _guess_receiver_goal_id(cursor, sender_goal_row, receiver_code, mbo_year, expected_muc_tieu):
    """
    Dò id record đã copy khi không có receiver_goal_id (fallback).
    Thêm điều kiện theo schema động:
      - nếu có mbo_year => lọc theo năm
      - nếu có phan_loai => ép phan_loai = 'nhan'
    """
    has_year = _table_has_column(cursor, "personalmbo", "mbo_year")
    has_phan_loai = _table_has_column(cursor, "personalmbo", "phan_loai")

    year_cond = "AND mbo_year = %s" if has_year else ""
    phanloai_cond = "AND phan_loai = 'nhan'" if has_phan_loai else ""

    sql = f"""
        SELECT id FROM personalmbo
        WHERE employee_code = %s
          {year_cond}
          AND ten_muc_tieu = %s AND mo_ta = %s
          AND don_vi_do_luong = %s
          AND IFNULL(gia_tri_ban_dau,'') = IFNULL(%s,'')
          AND IFNULL(han_hoan_thanh,'') = IFNULL(%s,'')
          AND muc_tieu = %s
          {phanloai_cond}
        ORDER BY id DESC
        LIMIT 1
    """

    params = [receiver_code]
    if has_year:
        params.append(mbo_year)
    params += [
        sender_goal_row["ten_muc_tieu"],
        sender_goal_row["mo_ta"],
        sender_goal_row["don_vi_do_luong"],
        sender_goal_row["gia_tri_ban_dau"],
        sender_goal_row["han_hoan_thanh"],
        str(expected_muc_tieu),
    ]

    cursor.execute(sql, params)
    row = cursor.fetchone()
    return row["id"] if row else None


# ======================
# Reset helpers (tích hợp vào transaction hiện tại)
# ======================
def _reset_mbo_to_draft_by_codes(cursor, employee_codes, mbo_year):
    """
    Reset các trường review/approve cho danh sách employee_code trong 1 năm
    và đưa session về 'draft'. Chạy trong cùng transaction (KHÔNG gọi HTTP).
    """
    codes = sorted({c for c in (employee_codes or []) if c})
    if not codes:
        return {"affected": {"competencymbo": 0, "personalmbo": 0, "sessions_drafted": 0},
                "employee_codes": [], "mbo_year": mbo_year}

    # Dò bảng có prefix schema hay không
    TABLE_COMP = "competencymbo"
    TABLE_PERS = "personalmbo"
    if _table_has_table(cursor, "nsh.competencymbo"):
        TABLE_COMP = "nsh.competencymbo"
    if _table_has_table(cursor, "nsh.personalmbo"):
        TABLE_PERS = "nsh.personalmbo"

    has_comp_year = _table_has_column(cursor, TABLE_COMP.split(".")[-1], "mbo_year")
    has_pers_year = _table_has_column(cursor, TABLE_PERS.split(".")[-1], "mbo_year")

    has_comp_rev = _table_has_column(cursor, TABLE_COMP.split(".")[-1], "reviewer_ti_trong")
    has_comp_app = _table_has_column(cursor, TABLE_COMP.split(".")[-1], "approver_ti_trong")
    has_pers_rev = _table_has_column(cursor, TABLE_PERS.split(".")[-1], "reviewer_ti_trong")
    has_pers_app = _table_has_column(cursor, TABLE_PERS.split(".")[-1], "approver_ti_trong")
    has_pers_rr  = _table_has_column(cursor, TABLE_PERS.split(".")[-1], "reviewer_rating")
    has_pers_ar  = _table_has_column(cursor, TABLE_PERS.split(".")[-1], "approver_rating")

    affected_comp = affected_pers = drafted = 0

    if codes:
        placeholders = ", ".join(["%s"] * len(codes))

        # Reset competencymbo
        if has_comp_rev and has_comp_app:
            where_year = "AND mbo_year = %s" if has_comp_year else ""
            sql_comp = f"""
                UPDATE {TABLE_COMP}
                SET reviewer_ti_trong = NULL,
                    approver_ti_trong = NULL
                WHERE employee_code IN ({placeholders})
                {where_year}
            """
            params = codes + ([mbo_year] if has_comp_year else [])
            cursor.execute(sql_comp, params)
            affected_comp = cursor.rowcount or 0

        # Reset personalmbo
        if has_pers_rev and has_pers_app and has_pers_rr and has_pers_ar:
            where_year = "AND mbo_year = %s" if has_pers_year else ""
            sql_pers = f"""
                UPDATE {TABLE_PERS}
                SET reviewer_ti_trong = NULL,
                    approver_ti_trong = NULL,
                    reviewer_rating   = NULL,
                    approver_rating   = NULL
                WHERE employee_code IN ({placeholders})
                {where_year}
            """
            params = codes + ([mbo_year] if has_pers_year else [])
            cursor.execute(sql_pers, params)
            affected_pers = cursor.rowcount or 0

        # Đưa session về draft (nếu có)
        cursor.execute(
            f"""
            SELECT id, employee_code FROM employees2026
            WHERE employee_code IN ({placeholders})
            """,
            codes,
        )
        rows = cursor.fetchall() or []
        emp_ids = [r["id"] for r in rows if r.get("id")]

        if emp_ids:
            placeholders_id = ", ".join(["%s"] * len(emp_ids))
            cursor.execute(
                f"""
                UPDATE mbo_sessions
                SET status='draft'
                WHERE employee_id IN ({placeholders_id})
                  AND mbo_year = %s
                  AND status <> 'draft'
                """,
                emp_ids + [mbo_year],
            )
            drafted = cursor.rowcount or 0

    return {
        "affected": {
            "competencymbo": int(affected_comp),
            "personalmbo": int(affected_pers),
            "sessions_drafted": int(drafted),
        },
        "employee_codes": codes,
        "mbo_year": mbo_year,
    }


# =========================
# Tạo danh sách phân bổ + copy mục tiêu (+ reset)
# =========================
@allocations_bp.route("/allocations", methods=["POST"])
def create_allocations():
    """
    Body: [
      { "goal_id": 123, "sender_code": "E001", "receiver_code": "E002",
        "allocation_value": "30" or "10%" or "12.5", "mbo_year": 2025 },
      ...
    ]
    """
    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({"error": "Dữ liệu không hợp lệ. Cần truyền vào một mảng các phân bổ."}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        has_receiver_goal = _has_receiver_goal_id(cursor)

        inserted, skipped, copied = 0, [], 0

        # Thu thập người nhận bị ảnh hưởng theo từng năm (để reset đúng năm)
        impacted_by_year = {}  # {mbo_year: set(receiver_codes)}

        for idx, item in enumerate(data):
            goal_id = item.get("goal_id")
            sender_code = item.get("sender_code")
            receiver_code = item.get("receiver_code")
            allocation_value = item.get("allocation_value")  # giữ KIỂU CHUỖI
            mbo_year = _require_mbo_year(item)

            if not all([goal_id, sender_code, receiver_code]) or allocation_value is None or mbo_year is None:
                skipped.append(idx)
                continue

            # 1) Lấy mục tiêu nguồn từ người gửi
            src = _fetch_sender_goal(cursor, goal_id, sender_code, mbo_year)
            if not src:
                skipped.append(idx)
                continue

            # 2) Copy sang người nhận (muc_tieu = allocation_value, phan_loai='nhan' nếu có)
            receiver_goal_id = _insert_receiver_goal(cursor, receiver_code, mbo_year, src, allocation_value)
            copied += 1

            # 3) Ghi phân bổ
            if has_receiver_goal:
                cursor.execute(
                    """
                    INSERT INTO mbo_allocations
                        (goal_id, sender_code, receiver_code, mbo_year, allocation_value, receiver_goal_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                    (goal_id, sender_code, receiver_code, mbo_year, str(allocation_value), receiver_goal_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO mbo_allocations
                        (goal_id, sender_code, receiver_code, mbo_year, allocation_value, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                    (goal_id, sender_code, receiver_code, mbo_year, str(allocation_value)),
                )

            inserted += 1
            impacted_by_year.setdefault(mbo_year, set()).add(receiver_code)

        # 4) Reset trạng thái cho các nhân viên nhận bị ảnh hưởng theo từng năm
        reset_summary = {}
        for year, codes in impacted_by_year.items():
            if codes:
                reset_summary[str(year)] = _reset_mbo_to_draft_by_codes(cursor, list(codes), year)

        conn.commit()
        return jsonify(
            {
                "message": "Thêm danh sách phân bổ thành công và đã copy mục tiêu.",
                "inserted": inserted,
                "copied_personalmbo": copied,
                "skipped_indexes": skipped,
                # bổ sung thông tin reset (không phá vỡ logic cũ)
                "reset": reset_summary,
            }
        ), 201

    except Exception as e:
        if conn:
            conn.rollback()
        print("Lỗi create_allocations:", repr(e))
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass


# =========================
# Cập nhật giá trị phân bổ + cập nhật muc_tieu người nhận (+ reset)
# =========================
@allocations_bp.route("/allocations/<int:allocation_id>", methods=["PUT", "PATCH"])
def update_allocation_value(allocation_id: int):
    """
    Body:
    {
      "allocation_value": "45" | "10%" | "12.5",   # BẮT BUỘC - KIỂU CHUỖI
      "sender_code": "E001"                        # optional: nếu truyền, kiểm tra đúng người gửi mới cho sửa
    }
    """
    payload = request.get_json() or {}
    if "allocation_value" not in payload:
        return jsonify({"error": "Thiếu allocation_value"}), 400

    allocation_value = str(payload.get("allocation_value") if payload.get("allocation_value") is not None else "")

    sender_code_cond = payload.get("sender_code")

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # 1) Lấy bản ghi phân bổ
        if sender_code_cond:
            cursor.execute(
                """
                SELECT id, goal_id, sender_code, receiver_code, mbo_year, allocation_value, 
                       created_at,
                       (SELECT COUNT(*) FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='mbo_allocations' AND COLUMN_NAME='receiver_goal_id') AS has_receiver_goal_id,
                       receiver_goal_id
                FROM mbo_allocations
                WHERE id = %s AND sender_code = %s
                LIMIT 1
            """,
                (allocation_id, sender_code_cond),
            )
        else:
            cursor.execute(
                """
                SELECT id, goal_id, sender_code, receiver_code, mbo_year, allocation_value, 
                       created_at,
                       (SELECT COUNT(*) FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='mbo_allocations' AND COLUMN_NAME='receiver_goal_id') AS has_receiver_goal_id,
                       receiver_goal_id
                FROM mbo_allocations
                WHERE id = %s
                LIMIT 1
            """,
                (allocation_id,),
            )

        alloc = cursor.fetchone()
        if not alloc:
            return jsonify({"error": "Không tìm thấy bản ghi phù hợp để cập nhật"}), 404

        # 2) Cập nhật allocation_value tại bảng phân bổ (giữ chuỗi)
        if sender_code_cond:
            cursor.execute(
                """
                UPDATE mbo_allocations
                SET allocation_value = %s
                WHERE id = %s AND sender_code = %s
            """,
                (allocation_value, allocation_id, sender_code_cond),
            )
        else:
            cursor.execute(
                """
                UPDATE mbo_allocations
                SET allocation_value = %s
                WHERE id = %s
            """,
                (allocation_value, allocation_id),
            )

        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "Không cập nhật được allocation_value"}), 400

        # 3) Cập nhật muc_tieu của goal đã copy cho người nhận
        receiver_goal_id = None
        has_receiver_goal = bool(alloc["has_receiver_goal_id"])

        if has_receiver_goal and alloc.get("receiver_goal_id"):
            receiver_goal_id = alloc["receiver_goal_id"]
        else:
            # Dự phòng: đoán id dựa theo fingerprint của goal nguồn
            src = _fetch_sender_goal(cursor, alloc["goal_id"], alloc["sender_code"], alloc["mbo_year"])
            if src:
                receiver_goal_id = _guess_receiver_goal_id(
                    cursor,
                    src,
                    alloc["receiver_code"],
                    alloc["mbo_year"],
                    expected_muc_tieu=alloc["allocation_value"],  # muc_tieu trước đó bằng allocation_value cũ (chuỗi)
                )

        if receiver_goal_id:
            cursor.execute(
                """
                UPDATE personalmbo
                SET muc_tieu = %s, updated_at = NOW()
                WHERE id = %s
            """,
                (allocation_value, receiver_goal_id),
            )
        else:
            conn.rollback()
            return jsonify({"error": "Không định vị được mục tiêu đã copy của người nhận để cập nhật"}), 500

        # 4) Reset trạng thái người nhận (không đổi logic trả về cũ)
        reset_info = _reset_mbo_to_draft_by_codes(cursor, [alloc["receiver_code"]], alloc["mbo_year"])

        conn.commit()
        return jsonify(
            {
                "message": "Cập nhật allocation_value và mục tiêu người nhận thành công",
                "id": allocation_id,
                "allocation_value": allocation_value,
                "receiver_goal_id": receiver_goal_id,
                # bổ sung để FE biết các nhân viên bị reset (không phá logic cũ)
                "reset": reset_info,
            }
        ), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print("Lỗi update_allocation_value:", repr(e))
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass


# =================
# Xoá phân bổ + xoá mục tiêu đã copy cho người nhận (+ reset)
# =================
@allocations_bp.route("/allocations/<int:allocation_id>", methods=["DELETE"])
def delete_allocation(allocation_id: int):
    """
    Body (optional):
    {
      "sender_code": "E001"   # optional: nếu truyền, chỉ cho phép xoá khi đúng người gửi
    }
    """
    payload = request.get_json(silent=True) or {}
    sender_code_cond = payload.get("sender_code")

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # 1) Lấy record phân bổ để biết receiver_goal_id / fallback info
        if sender_code_cond:
            cursor.execute(
                """
                SELECT id, goal_id, sender_code, receiver_code, mbo_year, allocation_value,
                       (SELECT COUNT(*) FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='mbo_allocations' AND COLUMN_NAME='receiver_goal_id') AS has_receiver_goal_id,
                       receiver_goal_id
                FROM mbo_allocations
                WHERE id = %s AND sender_code = %s
                LIMIT 1
            """,
                (allocation_id, sender_code_cond),
            )
        else:
            cursor.execute(
                """
                SELECT id, goal_id, sender_code, receiver_code, mbo_year, allocation_value,
                       (SELECT COUNT(*) FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='mbo_allocations' AND COLUMN_NAME='receiver_goal_id') AS has_receiver_goal_id,
                       receiver_goal_id
                FROM mbo_allocations
                WHERE id = %s
                LIMIT 1
            """,
                (allocation_id,),
            )
        alloc = cursor.fetchone()
        if not alloc:
            return jsonify({"error": "Không tìm thấy bản ghi để xoá"}), 404

        # 2) Tìm id mục tiêu đã copy bên personalmbo
        receiver_goal_id = None
        has_receiver_goal = bool(alloc["has_receiver_goal_id"])

        if has_receiver_goal and alloc.get("receiver_goal_id"):
            receiver_goal_id = alloc["receiver_goal_id"]
        else:
            # fallback đoán id
            src = _fetch_sender_goal(cursor, alloc["goal_id"], alloc["sender_code"], alloc["mbo_year"])
            if src:
                receiver_goal_id = _guess_receiver_goal_id(
                    cursor,
                    src,
                    alloc["receiver_code"],
                    alloc["mbo_year"],
                    expected_muc_tieu=alloc["allocation_value"],  # chuỗi
                )

        # 3) Xoá phân bổ
        if sender_code_cond:
            cursor.execute(
                "DELETE FROM mbo_allocations WHERE id = %s AND sender_code = %s",
                (allocation_id, sender_code_cond),
            )
        else:
            cursor.execute("DELETE FROM mbo_allocations WHERE id = %s", (allocation_id,))
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "Không xoá được phân bổ"}), 400

        # 4) Xoá mục tiêu đã copy (nếu tìm được)
        if receiver_goal_id:
            cursor.execute("DELETE FROM personalmbo WHERE id = %s", (receiver_goal_id,))

        # 5) Reset trạng thái người nhận
        reset_info = _reset_mbo_to_draft_by_codes(cursor, [alloc["receiver_code"]], alloc["mbo_year"])

        conn.commit()
        return jsonify(
            {
                "message": "Xoá phân bổ thành công"
                + (" và đã xoá mục tiêu đã copy" if receiver_goal_id else " (không tìm thấy mục tiêu để xoá)"),
                "id": allocation_id,
                "receiver_goal_id": receiver_goal_id,
                # bổ sung để FE biết các nhân viên bị reset
                "reset": reset_info,
            }
        ), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print("Lỗi delete_allocation:", repr(e))
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass


# =================
# Lấy danh sách phân bổ theo người gửi (GET/POST)
# =================
@allocations_bp.route("/allocations/by-sender", methods=["GET", "POST"])
def get_allocations_by_sender():
    """
    Hỗ trợ:
    - GET  /allocations/by-sender?sender_code=E001&mbo_year=2025&goal_id=123
    - POST /allocations/by-sender  (JSON hoặc form):
        {
          "sender_code": "E001",
          "mbo_year": 2025,
          "goal_id": 123   # optional
        }
    """
    # ---- lấy payload linh hoạt ----
    data = request.get_json(silent=True) or {}
    if not data:
        data = request.form.to_dict() or request.args.to_dict()

    sender_code = (data.get("sender_code") or "").strip()
    if not sender_code:
        return jsonify({"error": "Thiếu sender_code"}), 400

    mbo_year_raw = data.get("mbo_year")
    try:
        mbo_year = int(mbo_year_raw)
    except (TypeError, ValueError):
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (số trong 2000..2100)"}), 400
    if mbo_year < 2000 or mbo_year > 2100:
        return jsonify({"error": "mbo_year ngoài khoảng 2000..2100"}), 400

    goal_id = data.get("goal_id")
    try:
        if goal_id is not None and f"{goal_id}".strip() != "":
            goal_id = int(goal_id)
        else:
            goal_id = None
    except (TypeError, ValueError):
        return jsonify({"error": "goal_id phải là số nếu truyền"}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT a.id, a.goal_id, a.sender_code, a.receiver_code, a.mbo_year,
                   a.allocation_value, a.created_at,
                   e.full_name AS receiver_fullname
            FROM mbo_allocations a
            JOIN employees2026 e ON a.receiver_code = e.employee_code
            WHERE a.sender_code = %s
              AND a.mbo_year = %s
        """
        params = [sender_code, mbo_year]

        if goal_id is not None:
            query += " AND a.goal_id = %s"
            params.append(goal_id)

        query += " ORDER BY a.id DESC"

        cursor.execute(query, params)
        results = cursor.fetchall()

        return jsonify({
            "items": results,
            "sender_code": sender_code,
            "mbo_year": mbo_year,
            "count": len(results),
        }), 200

    except Exception as e:
        print("Lỗi get_allocations_by_sender:", repr(e))
        return jsonify({"error": "Internal Server Error", "detail": str(e)}), 500
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass
