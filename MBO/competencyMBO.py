from flask import Blueprint, request, jsonify
from database import get_connection

competency_bp = Blueprint("competency_bp", __name__)

# ---- helper chung: lấy & validate năm (CHỈ dùng 'mbo_year' ở body hoặc query) ----
def _require_mbo_year_from_body_or_query():
    """
    Đọc năm từ body JSON key 'mbo_year' hoặc query string '?mbo_year='.
    Chỉ chấp nhận 2000..2100. Không hỗ trợ key 'year'.
    """
    year = None
    if request.is_json:
        data = request.get_json(silent=True) or {}
        year = data.get("mbo_year")
    if year is None:
        year = request.args.get("mbo_year")
    try:
        year = int(year)
    except (TypeError, ValueError):
        return None
    if year < 2000 or year > 2100:
        return None
    return year


# -----------------------------
# Tạo mục tiêu (CREATE)
# -----------------------------
@competency_bp.route("/competency", methods=["POST"])
def create_competency():
    try:
        data = request.get_json() or {}
        employee_code = data.get("employee_code")
        goal_title = data.get("goal_title")
        goal_content = data.get("goal_content")
        ti_trong = data.get("ti_trong")  # có thể None
        mbo_year = _require_mbo_year_from_body_or_query()

        if not employee_code or not goal_title:
            return jsonify({"error": "Thiếu thông tin bắt buộc (employee_code hoặc goal_title)"}), 400
        if mbo_year is None:
            return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

        # Validate tỉ trọng
        try:
            ti_trong = float(ti_trong) if ti_trong is not None else 0
            if ti_trong < 0 or ti_trong > 100:
                return jsonify({"error": "Tỉ trọng phải từ 0 đến 100"}), 400
        except (ValueError, TypeError):
            return jsonify({"error": "Tỉ trọng phải là số hợp lệ"}), 400

        conn = get_connection()
        cursor = conn.cursor()

        sql = """
            INSERT INTO competencymbo
            (employee_code, mbo_year, goal_title, goal_content, ti_trong, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """
        cursor.execute(sql, (employee_code, mbo_year, goal_title, goal_content, ti_trong))
        conn.commit()

        return jsonify({"message": "Đã lưu mục tiêu thành công!", "mbo_year": mbo_year, "id": cursor.lastrowid}), 201

    except Exception as e:
        return jsonify({"error": f"Lỗi khi lưu mục tiêu: {str(e)}"}), 500

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


# -----------------------------
# Lấy danh sách mục tiêu theo mã NV + năm (READ LIST)
# -----------------------------
@competency_bp.route("/competency/<employee_code>", methods=["GET"])
def get_competency_list(employee_code):
    try:
        mbo_year = _require_mbo_year_from_body_or_query()
        if mbo_year is None:
            return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100). Truyền ?mbo_year=YYYY"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        sql = """
            SELECT
                id,
                employee_code,
                goal_title,
                goal_content,
                ti_trong,
                reviewer_ti_trong,
                approver_ti_trong,
                self_ey_content,
                self_ey_rating,

                -- các trường mới (EY - reviewed/approved)
                approved_ey_content,
                approved_ey_rating,
                approved_ey_score,
                reviewed_ey_content,
                reviewed_ey_rating,
                reviewed_ey_score,

                created_at
            FROM competencymbo
            WHERE employee_code = %s
              AND mbo_year = %s
            ORDER BY id DESC
        """
        cursor.execute(sql, (employee_code, mbo_year))
        rows = cursor.fetchall()

        return jsonify({"items": rows, "mbo_year": mbo_year}), 200

    except Exception as e:
        return jsonify({"error": f"Lỗi khi lấy danh sách mục tiêu: {str(e)}"}), 500

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
# -----------------------------
# Cập nhật mục tiêu theo id + employee_code + năm (UPDATE)
# -----------------------------
@competency_bp.route("/competency/<employee_code>/<int:id>", methods=["PUT"])
def update_competency(employee_code, id):
    try:
        data = request.get_json() or {}
        mbo_year = _require_mbo_year_from_body_or_query()
        if mbo_year is None:
            return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

        # Lấy dữ liệu từ request nếu có (cũ)
        goal_title          = data.get("goal_title")
        goal_content        = data.get("goal_content")
        ti_trong            = data.get("ti_trong")
        reviewer_ti_trong   = data.get("reviewer_ti_trong")
        approver_ti_trong   = data.get("approver_ti_trong")
        self_ey_content     = data.get("self_ey_content")
        self_ey_rating      = data.get("self_ey_rating")

        # Trường MỚI (EY)
        approved_ey_content = data.get("approved_ey_content")
        approved_ey_rating  = data.get("approved_ey_rating")   # su|h|st
        approved_ey_score   = data.get("approved_ey_score")
        reviewed_ey_content = data.get("reviewed_ey_content")
        reviewed_ey_rating  = data.get("reviewed_ey_rating")   # su|h|st
        reviewed_ey_score   = data.get("reviewed_ey_score")

        # Validate tỉ trọng (nếu gửi)
        def _validate_percent(val, field_name):
            if val is None:
                return None
            try:
                v = float(val)
            except (ValueError, TypeError):
                raise ValueError(f"{field_name} phải là số hợp lệ")
            if v < 0 or v > 100:
                raise ValueError(f"{field_name} phải từ 0 đến 100")
            return v

        try:
            ti_trong = _validate_percent(ti_trong, "Tỉ trọng")
            reviewer_ti_trong = _validate_percent(reviewer_ti_trong, "Tỉ trọng đánh giá")
            approver_ti_trong = _validate_percent(approver_ti_trong, "Tỉ trọng phê duyệt")
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400

        # Validate score (nếu gửi) – có thể điều chỉnh range theo yêu cầu
        def _validate_score(val, field_name):
            if val is None:
                return None
            try:
                v = float(val)
            except (ValueError, TypeError):
                raise ValueError(f"{field_name} phải là số hợp lệ")
            # giả định 0..100; nếu bạn muốn 0..10 thì đổi ở đây
            if v < 0 or v > 100:
                raise ValueError(f"{field_name} phải từ 0 đến 100")
            return v

        try:
            approved_ey_score = _validate_score(approved_ey_score, "approved_ey_score")
            reviewed_ey_score = _validate_score(reviewed_ey_score, "reviewed_ey_score")
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400

        if all(v is None for v in [
            goal_title, goal_content, ti_trong,
            reviewer_ti_trong, approver_ti_trong,
            self_ey_content, self_ey_rating,
            approved_ey_content, approved_ey_rating, approved_ey_score,
            reviewed_ey_content, reviewed_ey_rating, reviewed_ey_score
        ]):
            return jsonify({"error": "Không có trường nào để cập nhật"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Lấy bản ghi đúng năm
        cursor.execute(
            "SELECT * FROM competencymbo WHERE id = %s AND employee_code = %s AND mbo_year = %s",
            (id, employee_code, mbo_year)
        )
        current = cursor.fetchone()
        if not current:
            return jsonify({"error": "Không tìm thấy mục tiêu theo năm yêu cầu"}), 404

        # Merge giá trị
        updated_fields = {
            "goal_title":          goal_title          if goal_title          is not None else current["goal_title"],
            "goal_content":        goal_content        if goal_content        is not None else current["goal_content"],
            "ti_trong":            ti_trong            if ti_trong            is not None else current["ti_trong"],
            "reviewer_ti_trong":   reviewer_ti_trong   if reviewer_ti_trong   is not None else current.get("reviewer_ti_trong"),
            "approver_ti_trong":   approver_ti_trong   if approver_ti_trong   is not None else current.get("approver_ti_trong"),
            "self_ey_content":     self_ey_content     if self_ey_content     is not None else current.get("self_ey_content"),
            "self_ey_rating":      self_ey_rating      if self_ey_rating      is not None else current.get("self_ey_rating"),

            # MỚI
            "approved_ey_content": approved_ey_content if approved_ey_content is not None else current.get("approved_ey_content"),
            "approved_ey_rating":  approved_ey_rating  if approved_ey_rating  is not None else current.get("approved_ey_rating"),
            "approved_ey_score":   approved_ey_score   if approved_ey_score   is not None else current.get("approved_ey_score"),
            "reviewed_ey_content": reviewed_ey_content if reviewed_ey_content is not None else current.get("reviewed_ey_content"),
            "reviewed_ey_rating":  reviewed_ey_rating  if reviewed_ey_rating  is not None else current.get("reviewed_ey_rating"),
            "reviewed_ey_score":   reviewed_ey_score   if reviewed_ey_score   is not None else current.get("reviewed_ey_score"),
        }

        update_clause = ", ".join([f"{key} = %s" for key in updated_fields])
        values = list(updated_fields.values())

        sql = f"""
            UPDATE competencymbo
            SET {update_clause}
            WHERE id = %s AND employee_code = %s AND mbo_year = %s
        """
        values.extend([id, employee_code, mbo_year])

        cursor.execute(sql, values)
        conn.commit()

        return jsonify({"message": "Cập nhật thành công", "mbo_year": mbo_year}), 200

    except Exception as e:
        return jsonify({"error": f"Lỗi khi cập nhật mục tiêu: {str(e)}"}), 500

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()



# -----------------------------
# Xoá mục tiêu theo id + employee_code + năm (DELETE)
# -----------------------------
@competency_bp.route("/competency/<employee_code>/<int:id>", methods=["DELETE"])
def delete_competency(employee_code, id):
    try:
        mbo_year = _require_mbo_year_from_body_or_query()
        if mbo_year is None:
            return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

        conn = get_connection()
        cursor = conn.cursor()

        sql = """
            DELETE FROM competencymbo
            WHERE id = %s AND employee_code = %s AND mbo_year = %s
        """
        cursor.execute(sql, (id, employee_code, mbo_year))
        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"error": "Không tìm thấy mục tiêu theo năm yêu cầu"}), 404

        return jsonify({"message": "Xoá thành công", "mbo_year": mbo_year}), 200

    except Exception as e:
        return jsonify({"error": f"Lỗi khi xóa mục tiêu: {str(e)}"}), 500

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


# -----------------------------
# Tra cứu nội dung năng lực theo vị trí & tên năng lực (KHÔNG dính năm)
# -----------------------------
@competency_bp.route("/role_competency_content/filter", methods=["GET"])
def get_role_competency_by_position_and_name():
    try:
        position = request.args.get("position")
        competency_name = request.args.get("competency_name")

        if not position or not competency_name:
            return jsonify({"error": "Thiếu tham số position hoặc competency_name"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        sql = """
            SELECT description
            FROM role_competency_content
            WHERE position = %s
              AND competency_name = %s
        """
        cursor.execute(sql, (position, competency_name))
        rows = cursor.fetchall()

        return jsonify(rows), 200

    except Exception as e:
        return jsonify({"error": f"Lỗi khi lấy thông tin năng lực: {str(e)}"}), 500

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
