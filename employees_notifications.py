from flask import Blueprint, jsonify
from database import get_connection

employees_notifications_bp = Blueprint(
    "employees_notifications",
    __name__,
    url_prefix="/employees/notifications"
)

# ======================================================
# API 1: Lấy danh sách employee_id là Nhân viên (active)
# ======================================================
@employees_notifications_bp.route("/staff", methods=["GET"])
def get_active_staff_ids():
    """
    GET /employees/notifications/staff

    Trả về danh sách id của nhân viên:
    - position = 'Nhân viên'
    - employment_status = 'active'
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT id
            FROM nsh.employees2026_base
            WHERE position = %s
              AND employment_status = 'active'
            """,
            ("Nhân viên",)
        )

        rows = cur.fetchall()
        ids = [row["id"] for row in rows]

        return jsonify({
            "type": "staff",
            "count": len(ids),
            "employee_ids": ids
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
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


# ======================================================
# API 2: Lấy danh sách employee_id KHÔNG phải Nhân viên (active)
# ======================================================
@employees_notifications_bp.route("/managers", methods=["GET"])
def get_active_non_staff_ids():
    """
    GET /employees/notifications/managers

    Trả về danh sách id của nhân sự:
    - position != 'Nhân viên'
    - employment_status = 'active'
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT id
            FROM nsh.employees2026_base
            WHERE position <> %s
              AND employment_status = 'active'
            """,
            ("Nhân viên",)
        )

        rows = cur.fetchall()
        ids = [row["id"] for row in rows]

        return jsonify({
            "type": "non_staff",
            "count": len(ids),
            "employee_ids": ids
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
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
# ======================================================
# API 3: Lấy danh sách employee_id đang active (tất cả)
# ======================================================
@employees_notifications_bp.route("/active", methods=["GET"])
def get_all_active_employee_ids():
    """
    GET /employees/notifications/active

    Trả về danh sách id:
    - employment_status = 'active'
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT id
            FROM nsh.employees2026_base
            WHERE employment_status = 'active'
            """
        )

        rows = cur.fetchall()
        ids = [row["id"] for row in rows]

        return jsonify({
            "type": "all_active",
            "count": len(ids),
            "employee_ids": ids
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
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
