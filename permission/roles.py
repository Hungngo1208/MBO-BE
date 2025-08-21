from flask import Blueprint, request, jsonify
from database import get_connection

roles_bp = Blueprint("roles", __name__)

# -------------------------
# Helpers
# -------------------------
def _get_role_id(cur, role_id=None, role_name=None):
    """Resolve role_id từ id hoặc name."""
    if role_id:
        cur.execute("SELECT id FROM roles WHERE id=%s", (role_id,))  # đổi thành `db qlda`.roles nếu cần
        row = cur.fetchone()
        return row["id"] if row else None
    if role_name:
        cur.execute("SELECT id FROM roles WHERE name=%s", (role_name,))  # đổi thành `db qlda`.roles nếu cần
        row = cur.fetchone()
        return row["id"] if row else None
    return None

def _fetch_employee_roles(cur, employee_id):
    """Trả về danh sách role (id, name, description) của 1 employee."""
    cur.execute("""
        SELECT r.id, r.name, r.description
        FROM employee_roles er
        JOIN roles r ON r.id = er.role_id          -- đổi thành `db qlda`.roles nếu cần
        WHERE er.employee_id = %s
        ORDER BY r.name
    """, (employee_id,))
    return cur.fetchall()

# -------------------------
# GET /roles/employees
# Trả về nhân viên + roles (tên) + role_descriptions (mô tả) dạng chuỗi, dễ dùng cho FE hiện tại
# -------------------------
@roles_bp.route("/roles/employees", methods=["GET"])
def get_employees_with_roles():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                e.id AS employee_id,
                e.employee_code,
                e.full_name,
                e.position,
                GROUP_CONCAT(DISTINCT r.name ORDER BY r.name SEPARATOR ',') AS roles,
                GROUP_CONCAT(DISTINCT COALESCE(r.description, '') ORDER BY r.name SEPARATOR ',') AS role_descriptions
            FROM employee_roles er
            JOIN employees2026 e ON er.employee_id = e.id
            JOIN roles r ON er.role_id = r.id          -- đổi thành `db qlda`.roles nếu cần
            GROUP BY e.id, e.employee_code, e.full_name, e.position
            ORDER BY e.id
        """
        cursor.execute(query)
        results = cursor.fetchall()

        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# -------------------------
# GET /roles
# Trả về id, name, description cho popup cấp quyền
# -------------------------
@roles_bp.route("/roles", methods=["GET"])
def get_all_roles():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT id, name, description
            FROM roles                           -- đổi thành `db qlda`.roles nếu cần
            ORDER BY name
        """)
        roles = cursor.fetchall()

        return jsonify(roles)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# -------------------------
# POST /roles/add
# Body: { employee_id, role_id? , role_name? }
# -------------------------
@roles_bp.route("/roles/add", methods=["POST"])
def add_role_to_employee():
    conn = None
    cursor = None
    try:
        data = request.get_json(force=True)
        employee_id = data.get("employee_id")
        role_id = data.get("role_id")
        role_name = data.get("role_name")

        if not employee_id:
            return jsonify({"error": "Thiếu employee_id"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Kiểm tra employee
        cursor.execute("SELECT id FROM employees2026 WHERE id=%s", (employee_id,))
        if not cursor.fetchone():
            return jsonify({"error": "Không tìm thấy nhân viên"}), 404

        # Resolve role_id từ id hoặc name
        rid = _get_role_id(cursor, role_id=role_id, role_name=role_name)
        if not rid:
            return jsonify({"error": "Không tìm thấy vai trò (theo id hoặc name)"}), 404

        # Thêm idempotent
        cursor.execute("""
            INSERT IGNORE INTO employee_roles (employee_id, role_id)
            VALUES (%s, %s)
        """, (employee_id, rid))
        conn.commit()

        roles_now = _fetch_employee_roles(cursor, employee_id)
        return jsonify({
            "ok": True,
            "message": "Đã thêm vai trò cho nhân viên",
            "employee_id": employee_id,
            "roles": roles_now
        }), 200

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# -------------------------
# POST /roles/delete
# Body: { employee_id, role_id? , role_name? }
# -------------------------
@roles_bp.route("/roles/delete", methods=["POST"])
def delete_role_from_employee():
    conn = None
    cursor = None
    try:
        data = request.get_json(force=True)
        employee_id = data.get("employee_id")
        role_id = data.get("role_id")
        role_name = data.get("role_name")

        if not employee_id:
            return jsonify({"error": "Thiếu employee_id"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Kiểm tra employee
        cursor.execute("SELECT id FROM employees2026 WHERE id=%s", (employee_id,))
        if not cursor.fetchone():
            return jsonify({"error": "Không tìm thấy nhân viên"}), 404

        # Resolve role_id từ id hoặc name
        rid = _get_role_id(cursor, role_id=role_id, role_name=role_name)
        if not rid:
            return jsonify({"error": "Không tìm thấy vai trò (theo id hoặc name)"}), 404

        cursor.execute("""
            DELETE FROM employee_roles
            WHERE employee_id = %s AND role_id = %s
        """, (employee_id, rid))
        conn.commit()

        roles_now = _fetch_employee_roles(cursor, employee_id)
        return jsonify({
            "ok": True,
            "message": "Đã xoá vai trò khỏi nhân viên",
            "employee_id": employee_id,
            "roles": roles_now
        }), 200

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()
