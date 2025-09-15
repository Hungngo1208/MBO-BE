# status.py
from flask import Blueprint, jsonify, request
from mysql.connector import Error
from database import get_connection
from datetime import datetime

status_bp = Blueprint("status_bp", __name__)

@status_bp.get("/mbo/can-submit/<int:employee_id>")
def can_submit_mbo(employee_id: int):
    """
    Query param (optional):
      - year: int (mbo_year cần kiểm tra). Mặc định: năm hiện tại.

    Response:
    {
      "can_submit": true/false,
      "reason": "...",
      "manager_id": 123 | null,
      "manager_status": "draft" | ... | null,
      "mbo_year": 2025
    }
    """
    year = request.args.get("year", type=int) or datetime.now().year

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # 1) Lấy organization_unit_id của nhân viên
        cur.execute(
            """
            SELECT organization_unit_id
            FROM employees2026
            WHERE id = %s
            """,
            (employee_id,)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({
                "can_submit": False,
                "reason": f"Không tìm thấy employee_id={employee_id} trong employees2026.",
                "manager_id": None,
                "manager_status": None,
                "mbo_year": year
            }), 404

        org_unit_id = row["organization_unit_id"]

        if org_unit_id is None:
            # Không thuộc phòng ban => coi như cấp cao nhất
            return jsonify({
                "can_submit": True,
                "reason": "Nhân viên không thuộc phòng ban nào (organization_unit_id=NULL) — cho phép gửi.",
                "manager_id": None,
                "manager_status": None,
                "mbo_year": year
            })

        # 2) Tìm quản lý gần nhất KHÁC chính nhân viên (leo lên theo parent_id)
        #    MySQL 8+ cần WITH RECURSIVE
        cur.execute(
            """
            WITH RECURSIVE chain AS (
                SELECT id, parent_id, employee_id, 0 AS depth
                FROM organization_units
                WHERE id = %s
                UNION ALL
                SELECT ou.id, ou.parent_id, ou.employee_id, chain.depth + 1
                FROM organization_units ou
                JOIN chain ON ou.id = chain.parent_id
            )
            SELECT employee_id
            FROM chain
            WHERE employee_id IS NOT NULL
              AND employee_id <> %s
            ORDER BY depth
            LIMIT 1
            """,
            (org_unit_id, employee_id)
        )
        mgr = cur.fetchone()

        if not mgr:
            # Không tìm thấy quản lý phù hợp khi leo đến đỉnh
            return jsonify({
                "can_submit": True,
                "reason": "Không tìm thấy quản lý cấp trên (nhân viên là cấp cao nhất hoặc chưa gán quản lý) — cho phép gửi.",
                "manager_id": None,
                "manager_status": None,
                "mbo_year": year
            })

        manager_id = mgr["employee_id"]

        # 3) Kiểm tra trạng thái MBO của quản lý trong mbo_sessions cho năm tương ứng
        #    - Chỉ có các cột: id, employee_id, mbo_year, status, reviewer_id, approver_id, score_final
        #    - Lấy record mới nhất theo id DESC
        cur.execute(
            """
            SELECT status
            FROM mbo_sessions
            WHERE employee_id = %s
              AND mbo_year = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (manager_id, year)
        )
        sess = cur.fetchone()

        if not sess:
            # Không có bản ghi của cấp trên trong năm đó => chưa gửi
            return jsonify({
                "can_submit": False,
                "reason": f"Quản lý (employee_id={manager_id}) chưa có MBO session cho năm {year} — chưa được phép gửi.",
                "manager_id": manager_id,
                "manager_status": None,
                "mbo_year": year
            })

        manager_status = (sess.get("status") or "").strip().lower()

        if manager_status == "draft":
            return jsonify({
                "can_submit": False,
                "reason": f"Quản lý (employee_id={manager_id}) đang ở trạng thái 'draft' năm {year} — chưa được phép gửi.",
                "manager_id": manager_id,
                "manager_status": manager_status,
                "mbo_year": year
            })

        # Các trạng thái khác coi như đã gửi (submitted/reviewed/approved/…)
        return jsonify({
            "can_submit": True,
            "reason": f"Quản lý (employee_id={manager_id}) đã gửi MBO (status='{manager_status}') cho năm {year} — cho phép gửi.",
            "manager_id": manager_id,
            "manager_status": manager_status,
            "mbo_year": year
        })

    except Error as e:
        return jsonify({
            "can_submit": False,
            "reason": f"Lỗi cơ sở dữ liệu: {str(e)}",
            "manager_id": None,
            "manager_status": None,
            "mbo_year": year
        }), 500
    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except:
            pass
