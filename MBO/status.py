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
@status_bp.post("/mbo/force-draft/<int:employee_id>")
def force_draft(employee_id: int):
    """
    Đưa status MBO về 'draft' và reset các trường review/approve
    theo đúng employee_code + mbo_year. Không dùng start_transaction().
    """
    from mysql.connector import Error
    payload = request.get_json(silent=True) or {}
    year = (
        payload.get("mbo_year")
        or payload.get("year")
        or request.args.get("mbo_year", type=int)
        or request.args.get("year", type=int)
    )

    TABLE_COMP = "nsh.competencymbo"
    TABLE_PERS = "nsh.personalmbo"

    REQUIRED_COMP_COLS = ["employee_code", "mbo_year", "reviewer_ti_trong", "approver_ti_trong"]
    REQUIRED_PERS_COLS = ["employee_code", "mbo_year",
                          "reviewer_ti_trong", "approver_ti_trong",
                          "reviewer_rating", "approver_rating"]

    def check_table_cols(cur, schema_table, required_cols):
        schema, table = schema_table.split(".", 1) if "." in schema_table else (None, schema_table)
        params = [table]
        sql = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s
        """
        if schema:
            sql += " AND TABLE_SCHEMA = %s"
            params.append(schema)
        cur.execute(sql, tuple(params))
        have = {row["COLUMN_NAME"].lower() for row in cur.fetchall()}
        missing = [c for c in required_cols if c.lower() not in have]
        return missing

    conn = None
    cur = None
    try:
        conn = get_connection()
        conn.autocommit = False  # ✅ QUAN TRỌNG: tự quản lý transaction, KHÔNG dùng start_transaction
        cur = conn.cursor(dictionary=True)

        # 0) employee_code
        cur.execute("SELECT employee_code FROM employees2026 WHERE id=%s", (employee_id,))
        emp = cur.fetchone()
        if not emp or not emp.get("employee_code"):
            return jsonify({
                "ok": False, "employee_id": employee_id, "employee_code": None,
                "mbo_year": year, "message": f"Không tìm thấy employee_code cho employee_id={employee_id}."
            }), 404
        employee_code = emp["employee_code"]

        # 1) mbo_year
        if not year:
            cur.execute(
                """
                SELECT mbo_year
                FROM mbo_sessions
                WHERE employee_id=%s
                ORDER BY mbo_year DESC, id DESC
                LIMIT 1
                """,
                (employee_id,)
            )
            latest = cur.fetchone()
            if not latest or not latest.get("mbo_year"):
                return jsonify({
                    "ok": False, "employee_id": employee_id, "employee_code": employee_code,
                    "mbo_year": None, "message": "Không nhận được 'mbo_year' và cũng không tìm thấy phiên MBO để suy ra năm."
                }), 400
            year = int(latest["mbo_year"])

        # 2) session đúng năm
        cur.execute(
            """
            SELECT id, status, mbo_year
            FROM mbo_sessions
            WHERE employee_id=%s AND mbo_year=%s
            ORDER BY id DESC
            LIMIT 1
            """,
            (employee_id, year)
        )
        sess = cur.fetchone()
        if not sess:
            return jsonify({
                "ok": False, "employee_id": employee_id, "employee_code": employee_code,
                "mbo_year": year, "message": f"Không tìm thấy MBO session cho employee_id={employee_id} năm {year}."
            }), 404
        session_id = sess["id"]
        prev_status = (sess.get("status") or "").strip().lower()

        # 3) validate schema
        miss_comp = check_table_cols(cur, TABLE_COMP, REQUIRED_COMP_COLS)
        miss_pers = check_table_cols(cur, TABLE_PERS, REQUIRED_PERS_COLS)
        if miss_comp or miss_pers:
            return jsonify({
                "ok": False, "employee_id": employee_id, "employee_code": employee_code,
                "mbo_year": year, "session_id": session_id, "previous_status": prev_status,
                "missing_columns": {TABLE_COMP: miss_comp, TABLE_PERS: miss_pers},
                "message": "Thiếu cột bắt buộc ở bảng mục tiêu. Vui lòng đồng bộ schema."
            }), 400

        # 4) Transaction (autocommit=False, KHÔNG gọi start_transaction)
        # 4.1) Reset competencymbo
        cur.execute(
            f"""
            UPDATE {TABLE_COMP}
            SET reviewer_ti_trong = NULL,
                approver_ti_trong = NULL
            WHERE employee_code = %s
              AND mbo_year = %s
            """,
            (employee_code, year)
        )
        affected_comp = cur.rowcount

        # 4.2) Reset personalmbo
        cur.execute(
            f"""
            UPDATE {TABLE_PERS}
            SET reviewer_ti_trong = NULL,
                approver_ti_trong = NULL,
                reviewer_rating   = NULL,
                approver_rating   = NULL
            WHERE employee_code = %s
              AND mbo_year = %s
            """,
            (employee_code, year)
        )
        affected_personal = cur.rowcount

        # 4.3) Set session -> draft nếu cần
        if prev_status != "draft":
            cur.execute("UPDATE mbo_sessions SET status='draft' WHERE id=%s", (session_id,))

        conn.commit()  # ✅ commit toàn bộ

        return jsonify({
            "ok": True,
            "employee_id": employee_id,
            "employee_code": employee_code,
            "mbo_year": year,
            "session_id": session_id,
            "previous_status": prev_status,
            "new_status": "draft",
            "affected": {
                "competencymbo": int(affected_comp or 0),
                "personalmbo": int(affected_personal or 0),
            },
            "message": "Đã reset các trường đánh giá/duyệt theo đúng năm và đưa trạng thái MBO về 'draft' thành công."
        })

    except Error as e:
        if conn and conn.in_transaction:
            try: conn.rollback()
            except: pass
        return jsonify({
            "ok": False,
            "employee_id": employee_id,
            "mbo_year": year,
            "message": f"Lỗi cơ sở dữ liệu: {str(e)}"
        }), 500
    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except:
            pass
