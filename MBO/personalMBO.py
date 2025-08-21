from flask import Blueprint, request, jsonify
from database import get_connection

employees_bpp = Blueprint('employees_bp', __name__, url_prefix='/employees')

# --- helper: bắt buộc có năm 2000..2100, lấy từ body hoặc query (?year=) ---
def _require_mbo_year():
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


@employees_bpp.route('/muctieu', methods=['POST'])
def create_muctieu():
    data = request.json or {}
    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

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
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()


@employees_bpp.route('/muctieu/by-employee', methods=['POST'])
def get_muctieu_by_employee_post():
    data = request.json or {}
    employee_code = data.get('employee_code')
    if not employee_code:
        return jsonify({"error": "Thiếu mã nhân viên (employee_code)"}), 400

    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

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
        cursor.close()
        db.close()



@employees_bpp.route('/muctieu/<int:muctieu_id>', methods=['DELETE'])
def delete_muctieu(muctieu_id):
    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

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
        db.rollback()
        error_msg = str(e)
        if "1451" in error_msg:
            return jsonify({
                "error": "Không thể xoá mục tiêu vì đã được phân bổ cho nhân viên khác."
            }), 400
        return jsonify({"error": "Đã xảy ra lỗi hệ thống."}), 500
    finally:
        cursor.close()
        db.close()


@employees_bpp.route('/muctieu/<int:muctieu_id>', methods=['PUT'])
def update_muctieu(muctieu_id):
    data = request.json or {}
    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    try:
        db = get_connection()
        cursor = db.cursor(dictionary=True)

        # Kiểm tra mục tiêu đúng năm tồn tại
        cursor.execute("SELECT * FROM PersonalMBO WHERE id = %s AND mbo_year = %s", (muctieu_id, mbo_year))
        current = cursor.fetchone()
        if not current:
            return jsonify({"error": "Không tìm thấy mục tiêu theo năm yêu cầu"}), 404

        # Các trường cho phép cập nhật
        # Các trường cho phép cập nhật
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
        update_fields = []
        values = []

        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                values.append(data[field])

        if not update_fields:
            return jsonify({"error": "Không có trường nào để cập nhật"}), 400

        # luôn chạm updated_at
        update_fields.append("updated_at = NOW()")

        sql = f"""
        UPDATE PersonalMBO
        SET {', '.join(update_fields)}
        WHERE id = %s AND mbo_year = %s
        """
        values.extend([muctieu_id, mbo_year])

        cursor.execute(sql, values)
        db.commit()

        return jsonify({"message": "Cập nhật mục tiêu thành công", "mbo_year": mbo_year}), 200

    except Exception as e:
        db.rollback()
        print("Lỗi cập nhật mục tiêu:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()
