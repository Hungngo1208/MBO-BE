from flask import Blueprint, request, jsonify
from database import get_connection

employees_bpp = Blueprint('employees_bp', __name__, url_prefix='/employees')

# ======================
# Helpers chung
# ======================
def _require_mbo_year():
    """
    Lấy mbo_year từ body JSON hoặc querystring (?mbo_year=),
    hợp lệ khi là số 2000..2100. Không hợp lệ -> None.
    """
    year = None
    if request.is_json:
        year = (request.get_json(silent=True) or {}).get('mbo_year')
    if year is None:
        year = request.args.get('mbo_year')
    try:
        year = int(year)
    except (TypeError, ValueError):
        return None
    if year < 2000 or year > 2100:
        return None
    return year


def _table_has_column(cursor, table, column):
    """
    Kiểm tra bảng trong DB hiện tại có cột hay không.
    Lưu ý: dùng đúng tên bảng bạn đang sử dụng trong file này (PersonalMBO).
    """
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


def _fetch_sender_goal(cursor, goal_id, sender_code, mbo_year):
    """
    Lấy record nguồn từ PersonalMBO theo id + employee_code (+mbo_year nếu có cột).
    """
    has_year = _table_has_column(cursor, "PersonalMBO", "mbo_year")
    sql = f"""
        SELECT id, employee_code{', mbo_year' if has_year else ''},
               ten_muc_tieu, mo_ta, don_vi_do_luong,
               gia_tri_ban_dau, muc_tieu, han_hoan_thanh,
               created_at, updated_at
        FROM PersonalMBO
        WHERE id = %s AND employee_code = %s
        {"AND mbo_year = %s" if has_year else ""}
        LIMIT 1
    """
    params = [goal_id, sender_code]
    if has_year:
        params.append(mbo_year)
    cursor.execute(sql, params)
    return cursor.fetchone()


def _guess_receiver_goal_id(cursor, sender_goal_row, receiver_code, mbo_year, expected_muc_tieu):
    """
    Dò id record đã copy cho người nhận khi mbo_allocations không có receiver_goal_id.
    So khớp fingerprint của goal nguồn + allocation_value (muc_tieu bên người nhận).
    - Nếu bảng có cột mbo_year => thêm điều kiện lọc năm
    - Nếu bảng có cột phan_loai => ép phan_loai = 'nhan' (đánh dấu mục tiêu nhận)
    """
    has_year = _table_has_column(cursor, "PersonalMBO", "mbo_year")
    has_phan_loai = _table_has_column(cursor, "PersonalMBO", "phan_loai")

    year_cond = "AND mbo_year = %s" if has_year else ""
    phanloai_cond = "AND phan_loai = 'nhan'" if has_phan_loai else ""

    sql = f"""
        SELECT id FROM PersonalMBO
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
        sender_goal_row.get("ten_muc_tieu"),
        sender_goal_row.get("mo_ta"),
        sender_goal_row.get("don_vi_do_luong"),
        sender_goal_row.get("gia_tri_ban_dau"),
        sender_goal_row.get("han_hoan_thanh"),
        str(expected_muc_tieu),
    ]

    cursor.execute(sql, params)
    row = cursor.fetchone()
    return row["id"] if row else None


# ======================
# POST /employees/muctieu
# ======================
@employees_bpp.route('/muctieu', methods=['POST'])
def create_muctieu():
    data = request.json or {}
    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    db = None
    cursor = None
    try:
        db = get_connection()
        cursor = db.cursor()
        sql = """
        INSERT INTO PersonalMBO 
        (employee_code, mbo_year, ten_muc_tieu, mo_ta, don_vi_do_luong, ti_trong, gia_tri_ban_dau,
         muc_tieu, han_hoan_thanh, xep_loai, cap_do_theo_doi, phan_loai, phan_bo, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """
        cursor.execute(sql, (
            data.get('employee_code'),
            mbo_year,
            data.get('ten_muc_tieu'),
            data.get('mo_ta'),
            data.get('don_vi_do_luong'),
            data.get('ti_trong'),
            data.get('gia_tri_ban_dau'),
            data.get('muc_tieu'),
            data.get('han_hoan_thanh'),
            data.get('xep_loai'),
            data.get('cap_do_theo_doi'),
            data.get('phan_loai'),
            data.get('phan_bo'),
        ))
        db.commit()
        return jsonify({"message": "Tạo mục tiêu thành công", "id": cursor.lastrowid, "mbo_year": mbo_year}), 201
    except Exception as e:
        if db:
            db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor: cursor.close()
            if db: db.close()
        except Exception:
            pass


# ======================
# POST /employees/muctieu/by-employee
# ======================
@employees_bpp.route('/muctieu/by-employee', methods=['POST'])
def get_muctieu_by_employee_post():
    data = request.json or {}
    employee_code = data.get('employee_code')
    if not employee_code:
        return jsonify({"error": "Thiếu mã nhân viên (employee_code)"}), 400

    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    db = None
    cursor = None
    try:
        db = get_connection()
        cursor = db.cursor(dictionary=True)

        sql = """
        SELECT 
          id,
          employee_code,
          ten_muc_tieu,
          mo_ta,
          don_vi_do_luong,
          ti_trong,
          gia_tri_ban_dau,
          muc_tieu,
          han_hoan_thanh,
          xep_loai,
          cap_do_theo_doi,
          phan_loai,
          phan_bo,
          reviewer_ti_trong,
          approver_ti_trong,
          reviewer_rating,
          approver_rating,
          self_ey_content,
          self_ey_result,
          self_ey_rating,
          -- các trường mới (EY - reviewed/approved)
          approved_ey_content,
          approved_ey_result,
          approved_ey_rating,
          approved_ey_score,
          reviewed_ey_content,
          reviewed_ey_result,
          reviewed_ey_rating,
          reviewed_ey_score,
          created_at,
          updated_at
        FROM PersonalMBO
        WHERE employee_code = %s
          AND mbo_year = %s
        ORDER BY han_hoan_thanh ASC
        """
        cursor.execute(sql, (employee_code, mbo_year))
        results = cursor.fetchall()
        return jsonify({"data": results, "mbo_year": mbo_year}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor: cursor.close()
            if db: db.close()
        except Exception:
            pass


# ======================
# DELETE /employees/muctieu/<id>
# ======================
@employees_bpp.route('/muctieu/<int:muctieu_id>', methods=['DELETE'])
def delete_muctieu(muctieu_id):
    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    db = None
    cursor = None
    try:
        db = get_connection()
        cursor = db.cursor()

        cursor.execute("SELECT id FROM PersonalMBO WHERE id = %s AND mbo_year = %s", (muctieu_id, mbo_year))
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Không tìm thấy mục tiêu theo năm yêu cầu"}), 404

        cursor.execute("DELETE FROM PersonalMBO WHERE id = %s AND mbo_year = %s", (muctieu_id, mbo_year))
        db.commit()
        return jsonify({"message": "Xoá mục tiêu thành công", "mbo_year": mbo_year}), 200

    except Exception as e:
        if db:
            db.rollback()
        error_msg = str(e)
        if "1451" in error_msg:  # foreign key constraint (đã phân bổ)
            return jsonify({
                "error": "Không thể xoá mục tiêu vì đã được phân bổ cho nhân viên khác."
            }), 400
        return jsonify({"error": "Đã xảy ra lỗi hệ thống."}), 500
    finally:
        try:
            if cursor: cursor.close()
            if db: db.close()
        except Exception:
            pass


# ======================
# PUT /employees/muctieu/<id>  (kèm propagate sang mục tiêu đã nhận phân bổ)
# ======================
@employees_bpp.route('/muctieu/<int:muctieu_id>', methods=['PUT'])
def update_muctieu(muctieu_id):
    data = request.json or {}
    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    # Các trường cho phép cập nhật ở bản gốc (giữ như bạn đang dùng)
    allowed_fields = [
        'employee_code', 'ten_muc_tieu', 'mo_ta', 'don_vi_do_luong',
        'ti_trong', 'gia_tri_ban_dau', 'muc_tieu', 'han_hoan_thanh',
        'xep_loai', 'cap_do_theo_doi', 'phan_loai', 'phan_bo',
        'reviewer_ti_trong', 'approver_ti_trong',
        'reviewer_rating', 'approver_rating',
        'self_ey_content', 'self_ey_result', 'self_ey_rating',
        # Thêm các trường mới
        'approved_ey_content', 'approved_ey_result', 'approved_ey_rating', 'approved_ey_score',
        'reviewed_ey_content', 'reviewed_ey_result', 'reviewed_ey_rating', 'reviewed_ey_score'
    ]

    # Các trường được propagate sang mục tiêu đã copy của người nhận
    propagate_fields_whitelist = {
        'ten_muc_tieu', 'mo_ta', 'don_vi_do_luong', 'gia_tri_ban_dau', 'han_hoan_thanh'
    }

    db = None
    cursor = None
    try:
        db = get_connection()
        cursor = db.cursor(dictionary=True)

        # 0) Lấy mục tiêu hiện tại (bản gốc) để có fingerprint trước khi sửa
        cursor.execute("SELECT * FROM PersonalMBO WHERE id = %s AND mbo_year = %s", (muctieu_id, mbo_year))
        current = cursor.fetchone()
        if not current:
            return jsonify({"error": "Không tìm thấy mục tiêu theo năm yêu cầu"}), 404

        sender_code = current["employee_code"]

        # 1) Chuẩn bị UPDATE cho bản gốc
        update_fields = []
        values = []
        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                values.append(data[field])

        if not update_fields:
            return jsonify({"error": "Không có trường nào để cập nhật"}), 400

        update_fields.append("updated_at = NOW()")

        sql = f"""
        UPDATE PersonalMBO
        SET {', '.join(update_fields)}
        WHERE id = %s AND mbo_year = %s
        """
        values.extend([muctieu_id, mbo_year])

        # 2) Thực hiện update bản gốc
        cursor.execute(sql, values)

        # 3) Tính các trường cần propagate (chỉ các field mà client thực sự gửi và trong whitelist)
        propagate_updates = {k: v for k, v in data.items() if k in propagate_fields_whitelist}

        # Nếu không có gì để propagate -> commit & trả về
        if not propagate_updates:
            db.commit()
            return jsonify({"message": "Cập nhật mục tiêu thành công", "mbo_year": mbo_year, "propagated": 0}), 200

        # 4) Lấy danh sách phân bổ của mục tiêu này
        cursor.execute(
            """
            SELECT a.id, a.receiver_code, a.allocation_value, a.receiver_goal_id
            FROM mbo_allocations a
            WHERE a.goal_id = %s
              AND a.sender_code = %s
              AND a.mbo_year = %s
            """,
            (muctieu_id, sender_code, mbo_year),
        )
        allocations = cursor.fetchall() or []

        if not allocations:
            db.commit()
            return jsonify({"message": "Cập nhật mục tiêu thành công (không có phân bổ để đồng bộ)", "mbo_year": mbo_year, "propagated": 0}), 200

        # 5) Dò schema và chuẩn bị fingerprint trước khi sửa (từ current)
        has_receiver_goal = _has_receiver_goal_id(cursor)
        sender_goal_before = {
            "ten_muc_tieu": current.get("ten_muc_tieu"),
            "mo_ta": current.get("mo_ta"),
            "don_vi_do_luong": current.get("don_vi_do_luong"),
            "gia_tri_ban_dau": current.get("gia_tri_ban_dau"),
            "han_hoan_thanh": current.get("han_hoan_thanh"),
        }

        propagated_count = 0
        for alloc in allocations:
            receiver_goal_id = None

            if has_receiver_goal and alloc.get("receiver_goal_id"):
                receiver_goal_id = alloc["receiver_goal_id"]
            else:
                # Fallback: dò id bên PersonalMBO của người nhận dựa fingerprint + allocation_value (chính là muc_tieu)
                receiver_goal_id = _guess_receiver_goal_id(
                    cursor,
                    sender_goal_before,
                    alloc["receiver_code"],
                    mbo_year,
                    expected_muc_tieu=alloc["allocation_value"],
                )
                # Nếu tìm thấy và bảng có cột receiver_goal_id -> lưu bù để lần sau nhanh
                if receiver_goal_id and has_receiver_goal:
                    cursor.execute(
                        "UPDATE mbo_allocations SET receiver_goal_id = %s WHERE id = %s",
                        (receiver_goal_id, alloc["id"]),
                    )

            if not receiver_goal_id:
                # Không tìm thấy record đã copy để cập nhật -> bỏ qua
                continue

            # Build câu UPDATE cho mục tiêu của người nhận: chỉ chạm các field propagate + updated_at
            set_parts = [f"{k} = %s" for k in propagate_updates.keys()]
            vals = list(propagate_updates.values())
            set_parts.append("updated_at = NOW()")

            upd_sql = f"""
                UPDATE PersonalMBO
                SET {', '.join(set_parts)}
                WHERE id = %s
            """
            vals.append(receiver_goal_id)

            cursor.execute(upd_sql, vals)
            propagated_count += 1

        db.commit()
        return jsonify({
            "message": "Cập nhật mục tiêu thành công và đã đồng bộ mục tiêu đã nhận phân bổ",
            "mbo_year": mbo_year,
            "propagated": propagated_count
        }), 200

    except Exception as e:
        if db:
            db.rollback()
        print("Lỗi cập nhật mục tiêu (propagate):", repr(e))
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor: cursor.close()
            if db: db.close()
        except Exception:
            pass


# ======================
# POST /employees/muctieu/by-department
# ======================
@employees_bpp.route('/muctieu/by-department', methods=['POST'])
def get_muctieu_by_department():
    data = request.json or {}
    org_unit_id = data.get('organization_unit_id') or data.get('department_id')
    if org_unit_id is None:
        return jsonify({"error": "Thiếu organization_unit_id"}), 400

    try:
        org_unit_id = int(org_unit_id)
    except (TypeError, ValueError):
        return jsonify({"error": "organization_unit_id không hợp lệ"}), 400

    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    db = None
    cursor = None
    try:
        db = get_connection()
        cursor = db.cursor(dictionary=True)

        # ✅ Bỏ đệ quy — chỉ lấy đúng cấp phòng ban này
        sql = """
        SELECT
            p.id,
            p.employee_code,
            p.ten_muc_tieu,
            p.mo_ta,
            p.don_vi_do_luong,
            p.ti_trong,
            p.gia_tri_ban_dau,
            p.muc_tieu,
            p.han_hoan_thanh,
            p.xep_loai,
            p.cap_do_theo_doi,
            p.phan_loai,
            p.phan_bo,
            p.reviewer_ti_trong,
            p.approver_ti_trong,
            p.reviewer_rating,
            p.approver_rating,
            p.self_ey_content,
            p.self_ey_result,
            p.self_ey_rating,
            p.approved_ey_content,
            p.approved_ey_result,
            p.approved_ey_rating,
            p.approved_ey_score,
            p.reviewed_ey_content,
            p.reviewed_ey_result,
            p.reviewed_ey_rating,
            p.reviewed_ey_score,
            p.created_at,
            p.updated_at,

            -- Thông tin nhân viên/phòng ban
            e.full_name,
            e.position,
            e.organization_unit_id,
            ou.name AS department_name,
            ou.type AS department_type
        FROM PersonalMBO p
        INNER JOIN employees2026 e
            ON e.employee_code = p.employee_code
        INNER JOIN organization_units ou
            ON ou.id = e.organization_unit_id
        WHERE p.mbo_year = %s
          AND e.organization_unit_id = %s
          AND (
                LOWER(COALESCE(p.cap_do_theo_doi, '')) = 'phongban'
             OR LOWER(COALESCE(p.cap_do_theo_doi, '')) = 'congty'
          )
        ORDER BY e.full_name, p.han_hoan_thanh ASC
        """
        cursor.execute(sql, (mbo_year, org_unit_id))
        rows = cursor.fetchall()

        return jsonify({
            "data": rows,
            "mbo_year": mbo_year,
            "organization_unit_id": org_unit_id,
            "count": len(rows)
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cursor: cursor.close()
            if db: db.close()
        except Exception:
            pass
