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

    try:
        db = get_connection()
        cursor = db.cursor(dictionary=True)

        # Lấy tất cả phòng ban con (bao gồm chính nó) bằng CTE đệ quy (MySQL 8+)
        # Nếu DB chưa bật recursive CTE, mình có thể viết phiên bản vòng lặp Python—bạn bảo mình nếu cần.
        sql = """
        WITH RECURSIVE ou_all AS (
            SELECT id, parent_id, name, type
            FROM organization_units
            WHERE id = %s
            UNION ALL
            SELECT ou.id, ou.parent_id, ou.name, ou.type
            FROM organization_units ou
            INNER JOIN ou_all a ON ou.parent_id = a.id
        )
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

            -- Thông tin nhân viên/phòng ban để hiển thị
            e.full_name,
            e.position,
            e.organization_unit_id,
            ou_all.name AS department_name,
            ou_all.type AS department_type
        FROM PersonalMBO p
        INNER JOIN employees2026 e
            ON e.employee_code = p.employee_code
        INNER JOIN ou_all
            ON ou_all.id = e.organization_unit_id
        WHERE p.mbo_year = %s
          AND LOWER(COALESCE(p.cap_do_theo_doi, '')) = 'phongban'
        ORDER BY ou_all.id, e.full_name, p.han_hoan_thanh ASC
        """
        cursor.execute(sql, (org_unit_id, mbo_year))
        rows = cursor.fetchall()

        return jsonify({
            "data": rows,
            "mbo_year": mbo_year,
            "root_organization_unit_id": org_unit_id,
            "count": len(rows)
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cursor.close()
            db.close()
        except Exception:
            pass
@employees_bpp.route('/muctieu/by-company-auto', methods=['POST'])
def get_muctieu_by_company_auto():
    """
    Body JSON:
    {
      "organization_unit_id": 123,
      "mbo_year": 2025
    }
    """
    data = request.json or {}
    orig_id = data.get('organization_unit_id') or data.get('department_id') or data.get('company_id')
    if orig_id is None:
        return jsonify({"error": "Thiếu organization_unit_id"}), 400

    try:
        orig_id = int(orig_id)
    except (TypeError, ValueError):
        return jsonify({"error": "organization_unit_id không hợp lệ"}), 400

    mbo_year = _require_mbo_year()
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    try:
        db = get_connection()
        cursor = db.cursor(dictionary=True)

        # === B1. Lấy node gốc và loại ===
        cursor.execute("""
            SELECT id, parent_id, name, LOWER(COALESCE(type,'')) AS type
            FROM organization_units WHERE id=%s
        """, (orig_id,))
        orig = cursor.fetchone()
        if not orig:
            return jsonify({"error": "organization_unit_id không tồn tại"}), 404

        orig_type = orig["type"]

        # Tìm corporation ancestor cao nhất của orig_id (dùng cho cả 2 trường hợp)
        cursor.execute("""
            WITH RECURSIVE up AS (
              SELECT id, parent_id, name, LOWER(COALESCE(type,'')) AS type, 0 depth
              FROM organization_units WHERE id=%s
              UNION ALL
              SELECT p.id, p.parent_id, p.name, LOWER(COALESCE(p.type,'')) AS type, up.depth+1
              FROM organization_units p JOIN up ON p.id = up.parent_id
            )
            SELECT id, name FROM up
            WHERE type='corporation'
            ORDER BY depth ASC LIMIT 1
        """, (orig_id,))
        corp_ancestor = cursor.fetchone()
        if not corp_ancestor:
            return jsonify({"error": "Không tìm thấy corporation tổ tiên"}), 404

        # === B2. Hai trường hợp ghi nhớ ID ===

        # Trường hợp 1: orig là corporation
        if orig_type == 'corporation':
            corp_id = orig["id"]
            corp_name = orig["name"]

            # Lấy tất cả company con (mọi cấp) của corporation này
            cursor.execute("""
                WITH RECURSIVE tree AS (
                  SELECT id, parent_id, name, LOWER(COALESCE(type,'')) AS type
                  FROM organization_units WHERE id=%s
                  UNION ALL
                  SELECT ou.id, ou.parent_id, ou.name, LOWER(COALESCE(ou.type,'')) AS type
                  FROM organization_units ou
                  JOIN tree t ON ou.parent_id = t.id
                )
                SELECT id, name FROM tree WHERE type='company'
            """, (corp_id,))
            company_rows = cursor.fetchall()
            company_ids = [r["id"] for r in company_rows]  # có thể rỗng nếu chưa có company con

            # Quét mục tiêu: chỉ canhan + congty + đúng năm + thuộc corporation này (self hoặc company con)
            cursor.execute("""
                WITH RECURSIVE
                chain AS (
                  SELECT id, parent_id, name, LOWER(COALESCE(type,'')) AS type, id AS start_id, 0 AS depth
                  FROM organization_units
                  UNION ALL
                  SELECT p.id, p.parent_id, p.name, LOWER(COALESCE(p.type,'')) AS type, c.start_id, c.depth+1
                  FROM organization_units p
                  JOIN chain c ON p.id = c.parent_id
                ),
                nearest_company AS (
                  SELECT start_id AS unit_id, id AS company_id, name AS company_name
                  FROM (
                    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY depth) rn
                    FROM chain c WHERE c.type='company'
                  ) t WHERE rn=1
                ),
                nearest_corporation AS (
                  SELECT start_id AS unit_id, id AS corp_id, name AS corp_name
                  FROM (
                    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY depth) rn
                    FROM chain c WHERE c.type='corporation'
                  ) t WHERE rn=1
                )
                SELECT
                  p.*,
                  e.full_name, e.position,
                  e.organization_unit_id AS employee_department_id,
                  ou.name AS employee_department_name,
                  LOWER(COALESCE(ou.type,'')) AS employee_department_type,
                  nc.company_id   AS nearest_company_id,
                  nc.company_name AS nearest_company_name,
                  nco.corp_id     AS nearest_corp_id,
                  nco.corp_name   AS nearest_corp_name
                FROM PersonalMBO p
                JOIN employees2026 e ON e.employee_code = p.employee_code
                JOIN organization_units ou ON ou.id = e.organization_unit_id
                LEFT JOIN nearest_company nc ON nc.unit_id = e.organization_unit_id
                LEFT JOIN nearest_corporation nco ON nco.unit_id = e.organization_unit_id
                WHERE p.mbo_year = %s
                  AND LOWER(COALESCE(p.phan_loai,''))='canhan'
                  AND LOWER(COALESCE(p.cap_do_theo_doi,''))='congty'
                  AND nco.corp_id = %s  -- chỉ các mục tiêu thuộc tập đoàn này
            """, (mbo_year, corp_id))
            all_rows = cursor.fetchall()

            # Chia nhóm: corporation self (nearest_company_id IS NULL) & từng company con
            corp_rows = [r for r in all_rows if (r["nearest_corp_id"] == corp_id and r["nearest_company_id"] is None)]

            by_company = {}
            for r in all_rows:
                cid = r["nearest_company_id"]
                if cid is None:
                    continue  # đã thuộc nhóm corporation self
                # Chỉ giữ company là con của corp này (an toàn nếu DB có dữ liệu chéo)
                if company_ids and cid not in company_ids:
                    continue
                cname = r["nearest_company_name"]
                by_company.setdefault((cid, cname), []).append(r)

            # Lọc bỏ company không có mục tiêu (count=0)
            companies_payload = [
                {
                    "id": cid,
                    "name": cname,
                    "type": "company",
                    "count": len(items),
                    "data": items
                }
                for (cid, cname), items in by_company.items()
                if len(items) > 0
            ]

            payload = {
                "mbo_year": mbo_year,
                "corporation": {
                    "id": corp_id,
                    "name": corp_name,
                    "type": "corporation",
                    "count": len(corp_rows),
                    "data": corp_rows
                },
                "companies": companies_payload  # đã lọc bỏ company rỗng
            }
            return jsonify(payload), 200

        # Trường hợp 2: orig KHÔNG phải corporation → lấy company gần nhất & corporation tương ứng
        else:
            # nearest company của orig_id
            cursor.execute("""
                WITH RECURSIVE up AS (
                  SELECT id, parent_id, name, LOWER(COALESCE(type,'')) AS type, 0 depth
                  FROM organization_units WHERE id=%s
                  UNION ALL
                  SELECT p.id, p.parent_id, p.name, LOWER(COALESCE(p.type,'')) AS type, up.depth+1
                  FROM organization_units p JOIN up ON p.id = up.parent_id
                )
                SELECT id, name FROM up
                WHERE type='company'
                ORDER BY depth ASC LIMIT 1
            """, (orig_id,))
            company_ancestor = cursor.fetchone()
            if not company_ancestor:
                return jsonify({"error": "Không tìm thấy company ancestor gần nhất"}), 404

            company_id, company_name = company_ancestor["id"], company_ancestor["name"]
            corp_id, corp_name = corp_ancestor["id"], corp_ancestor["name"]

            # Quét mục tiêu: canhan + congty + năm; chọn theo:
            # - Company block: nearest_company_id = company_id
            # - Corporation self block: nearest_corp_id = corp_id AND nearest_company_id IS NULL
            cursor.execute("""
                WITH RECURSIVE
                chain AS (
                  SELECT id, parent_id, name, LOWER(COALESCE(type,'')) AS type, id AS start_id, 0 AS depth
                  FROM organization_units
                  UNION ALL
                  SELECT p.id, p.parent_id, p.name, LOWER(COALESCE(p.type,'')) AS type, c.start_id, c.depth+1
                  FROM organization_units p
                  JOIN chain c ON p.id = c.parent_id
                ),
                nearest_company AS (
                  SELECT start_id AS unit_id, id AS company_id, name AS company_name
                  FROM (
                    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY depth) rn
                    FROM chain c WHERE c.type='company'
                  ) t WHERE rn=1
                ),
                nearest_corporation AS (
                  SELECT start_id AS unit_id, id AS corp_id, name AS corp_name
                  FROM (
                    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY depth) rn
                    FROM chain c WHERE c.type='corporation'
                  ) t WHERE rn=1
                )
                SELECT
                  p.*,
                  e.full_name, e.position,
                  e.organization_unit_id AS employee_department_id,
                  ou.name AS employee_department_name,
                  LOWER(COALESCE(ou.type,'')) AS employee_department_type,
                  nc.company_id   AS nearest_company_id,
                  nc.company_name AS nearest_company_name,
                  nco.corp_id     AS nearest_corp_id,
                  nco.corp_name   AS nearest_corp_name
                FROM PersonalMBO p
                JOIN employees2026 e ON e.employee_code = p.employee_code
                JOIN organization_units ou ON ou.id = e.organization_unit_id
                LEFT JOIN nearest_company nc ON nc.unit_id = e.organization_unit_id
                LEFT JOIN nearest_corporation nco ON nco.unit_id = e.organization_unit_id
                WHERE p.mbo_year = %s
                  AND LOWER(COALESCE(p.phan_loai,''))='canhan'
                  AND LOWER(COALESCE(p.cap_do_theo_doi,''))='congty'
                  AND (
                       (nc.company_id = %s) -- company block
                    OR (nco.corp_id = %s AND nc.company_id IS NULL) -- corporation self block
                  )
            """, (mbo_year, company_id, corp_id))
            rows = cursor.fetchall()

            # Chia 2 block
            company_rows = [r for r in rows if r["nearest_company_id"] == company_id]
            corp_rows    = [r for r in rows if (r["nearest_corp_id"] == corp_id and r["nearest_company_id"] is None)]

            # Nếu company không có mục tiêu → không trả block company
            payload = {
                "mbo_year": mbo_year,
                "corporation": {
                    "id": corp_id,
                    "name": corp_name,
                    "type": "corporation",
                    "count": len(corp_rows),
                    "data": corp_rows
                }
            }
            if len(company_rows) > 0:
                payload["company"] = {
                    "id": company_id,
                    "name": company_name,
                    "type": "company",
                    "count": len(company_rows),
                    "data": company_rows
                }

            return jsonify(payload), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cursor.close()
            db.close()
        except Exception:
            pass
