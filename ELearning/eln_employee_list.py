# eln_employee_list.py
# -*- coding: utf-8 -*-
from flask import Blueprint, request, jsonify
from decimal import Decimal
from database import get_connection

eln_employee_bp = Blueprint("eln_employee_bp", __name__)

@eln_employee_bp.get("/eln/employees-with-status")
def employees_with_status():
    # Lấy limit/offset an toàn
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset phải là số nguyên"}), 400

    sql = """
        SELECT
            e.id                                   AS employee_id,
            e.full_name                            AS ten_nhan_vien,
            e.vi_tri                                AS vi_tri,
            e.entry_date                           AS ngay_vao_cong_ty,
            e.organization_unit_id                 AS organization_unit_id,
            ou.name                                AS bo_phan,
            s.hien_trang                           AS hien_trang,
            COALESCE(s.tong_so_mon_hoc, 0)         AS tong_so_mon_hoc,
            COALESCE(s.so_mon_hoc_hoan_thanh, 0)   AS so_mon_hoc_hoan_thanh
        FROM nsh.employees2026_base e
        LEFT JOIN nsh.organization_units ou
            ON ou.id = e.organization_unit_id
        LEFT JOIN nsh.eln_employee_status s
            ON s.employee_id = e.id
        WHERE e.employment_status = 'active'
        ORDER BY e.full_name, e.id
        LIMIT %s OFFSET %s
    """

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, (limit, offset))
        rows = cur.fetchall()

        # Chuẩn hoá kiểu ngày cho JSON
        for r in rows:
            v = r.get("ngay_vao_cong_ty")
            if v is not None:
                try:
                    r["ngay_vao_cong_ty"] = v.isoformat()
                except Exception:
                    r["ngay_vao_cong_ty"] = str(v)

        return jsonify(rows)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
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

@eln_employee_bp.get("/eln/employee-courses")
def get_employee_courses():
    """
    API trả về danh sách các khoá học gắn với nhân viên trong bảng nsh.eln_employee_courses
    Các trường: id, employee_id, course_id, gan_nhat, ngay, ket_qua, hien_trang, thoi_gian_yeu_cau
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        sql = """
            SELECT
                id,
                employee_id,
                course_id,
                gan_nhat,
                ngay,
                ket_qua,
                hien_trang,
                thoi_gian_yeu_cau
            FROM nsh.eln_employee_courses
            ORDER BY id DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()

        # Chuẩn hoá kiểu dữ liệu cho JSON (date/datetime, decimal)
        for r in rows:
            for key, val in r.items():
                if isinstance(val, Decimal):
                    r[key] = float(val)
                elif hasattr(val, "isoformat"):
                    r[key] = val.isoformat()

        return jsonify(rows)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
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
@eln_employee_bp.get("/eln/employee-courses/<int:employee_id>")
def get_employee_courses_by_employee(employee_id: int):
    """
    Trả về danh sách các môn học (nsh.eln_employee_courses) của một nhân viên theo employee_id.
    JOIN với bảng nsh.eln để lấy tên khóa học (eln.title).
    Các trường: id, employee_id, course_id, ten_khoa_hoc, gan_nhat, ngay, ket_qua, hien_trang, thoi_gian_yeu_cau
    Hỗ trợ phân trang: ?limit=..., ?offset=...
    """
    # Đọc limit/offset
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset phải là số nguyên"}), 400

    sql = """
        SELECT
            c.id,
            c.employee_id,
            c.course_id,
            e.title AS ten_khoa_hoc,
            c.gan_nhat,
            c.ngay,
            c.ket_qua,
            c.hien_trang,
            c.thoi_gian_yeu_cau
        FROM nsh.eln_employee_courses AS c
        LEFT JOIN nsh.eln AS e
               ON e.id = c.course_id
        WHERE c.employee_id = %s
        ORDER BY COALESCE(c.gan_nhat, 0) DESC, c.ngay DESC, c.id DESC
        LIMIT %s OFFSET %s
    """

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, (employee_id, limit, offset))
        rows = cur.fetchall()

        # Chuẩn hoá kiểu dữ liệu cho JSON
        for r in rows:
            for k, v in list(r.items()):
                # Decimal -> float
                if isinstance(v, Decimal):
                    r[k] = float(v)
                # date/datetime -> ISO string
                elif hasattr(v, "isoformat"):
                    r[k] = v.isoformat()

        return jsonify(rows)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
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
# ============================= API 4 (MỚI - cập nhật theo yêu cầu) =============================
@eln_employee_bp.get("/eln/course-employees")
def get_employees_by_course():
    """
    Trả về danh sách NHÂN VIÊN theo course_id dựa trên bảng nsh.eln_employee_courses,
    và JOIN sang bảng nsh.employees2026_base bằng employee_id để lấy thông tin nhân viên
    (vi_tri, entry_date, organization_unit_id, bo_phan).

    Query params:
      - course_id (bắt buộc)
      - status (tuỳ chọn): lọc theo trạng thái trong nsh.eln_employee_courses.status
      - limit, offset (tuỳ chọn, mặc định 100 và 0)

    Kết quả trả về (ví dụ):
      [
        {
          "eln_employee_course_id": 1,
          "course_id": 123,
          "ten_khoa_hoc": "An toàn lao động",
          "employee_id": 456,
          "employee_code": "E0001",
          "full_name": "Nguyễn Văn A",
          "vi_tri": "Kỹ sư",
          "entry_date": "2024-03-01",
          "organization_unit_id": 99,
          "bo_phan": "Nhà máy A",
          "gan_nhat": 1,
          "ngay": "2025-09-20",
          "ket_qua": "85",
          "hien_trang": "đã học",
          "thoi_gian_yeu_cau": 8,
          "status": "pass"
        },
        ...
      ]
    """
    # Lấy course_id
    course_id = request.args.get("course_id")
    if not course_id:
        return jsonify({"error": "Thiếu tham số course_id"}), 400

    # Lấy status (optional), limit/offset
    status = request.args.get("status")
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset phải là số nguyên"}), 400

    # SQL: JOIN employees2026_base để lấy vi_tri, entry_date, bộ phận
    base_sql = """
        SELECT
            c.id                               AS eln_employee_course_id,
            c.course_id,
            eln.title                          AS ten_khoa_hoc,
            c.employee_id,
            eb.employee_code,
            eb.full_name,
            eb.vi_tri,
            eb.entry_date,
            eb.organization_unit_id,
            ou.name                            AS bo_phan,
            c.gan_nhat,
            c.ngay,
            c.ket_qua,
            c.hien_trang,
            c.thoi_gian_yeu_cau,
            c.status
        FROM nsh.eln_employee_courses c
        JOIN nsh.employees2026_base eb
              ON eb.id = c.employee_id
        LEFT JOIN nsh.organization_units ou
              ON ou.id = eb.organization_unit_id
        LEFT JOIN nsh.eln eln
              ON eln.id = c.course_id
        WHERE c.course_id = %s
    """
    params = [course_id]

    if status:
        base_sql += " AND c.status = %s"
        params.append(status)

    base_sql += """
        ORDER BY eb.full_name, eb.id
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(base_sql, params)
        rows = cur.fetchall()

        # Chuẩn hoá dữ liệu trả về (Decimal -> float, date/datetime -> ISO string)
        for r in rows:
            # Chuẩn hoá entry_date và ngay
            for k in ["entry_date", "ngay"]:
                v = r.get(k)
                if hasattr(v, "isoformat"):
                    try:
                        r[k] = v.isoformat()
                    except Exception:
                        r[k] = str(v)

            # Các trường số thập phân (nếu có)
            for k, v in list(r.items()):
                from decimal import Decimal as _D
                if isinstance(v, _D):
                    r[k] = float(v)

        return jsonify(rows)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
    finally:
        try:
            if cur: cur.close()
        except:
            pass
        try:
            if conn: conn.close()
        except:
            pass

