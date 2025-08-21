from flask import Blueprint, request, jsonify
from database import get_connection

permission_bp = Blueprint('permission', __name__, url_prefix='/api/permissions')

# Lấy quyền theo username
@permission_bp.route('/by-username/<username>', methods=['GET'])
def get_permissions_by_username(username):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Lấy quyền từ bảng users
        cursor.execute(
            "SELECT username, project_permission FROM users WHERE username = %s",
            (username,)
        )
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "Người dùng không tồn tại trong bảng users"}), 404

        permissions = user['project_permission'].split(",") if user['project_permission'] else []

        # Lấy thông tin nhân viên từ bảng employees dựa vào code = username
        cursor.execute(
            "SELECT full_name, code, department, position, company FROM employees WHERE code = %s",
            (username,)
        )
        employee = cursor.fetchone()
        if not employee:
            return jsonify({"error": "Người dùng không tồn tại trong bảng employees"}), 404

        result = {
            "username": user["username"],
            "permissions": permissions,
            "full_name": employee["full_name"],
            "code": employee["code"],
            "department": employee["department"],
            "position": employee["position"],
            "company": employee["company"]
        }
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Cập nhật toàn bộ quyền (thay thế)
@permission_bp.route('/update', methods=['PUT'])
def update_permission():
    data = request.get_json()
    username = data.get('username')
    permissions = data.get('permissions')

    if not username or permissions is None or not isinstance(permissions, list):
        return jsonify({"error": "Thiếu hoặc sai định dạng username hoặc permissions"}), 400

    permission_str = ",".join(sorted(set(permissions)))

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET project_permission = %s WHERE username = %s",
            (permission_str, username)
        )
        conn.commit()
        return jsonify({"message": "Cập nhật quyền thành công!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Thêm quyền mới vào danh sách (nếu chưa có)
@permission_bp.route('/add', methods=['POST'])
def add_permission():
    data = request.get_json()
    username = data.get('username')
    permission_to_add = data.get('permission')

    if not username or not permission_to_add:
        return jsonify({"error": "Thiếu username hoặc permission"}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT project_permission FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "Người dùng không tồn tại"}), 404

        permissions = user['project_permission'].split(",") if user['project_permission'] else []

        if permission_to_add in permissions:
            return jsonify({"message": "Quyền đã tồn tại"}), 200

        permissions.append(permission_to_add)
        permission_str = ",".join(sorted(set(permissions)))

        cursor.execute("UPDATE users SET project_permission = %s WHERE username = %s", (permission_str, username))
        conn.commit()
        return jsonify({"message": "Thêm quyền thành công!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Xoá quyền khỏi danh sách (nếu có)
@permission_bp.route('/remove', methods=['POST'])
def remove_permission():
    data = request.get_json()
    username = data.get('username')
    permission_to_remove = data.get('permission')

    if not username or not permission_to_remove:
        return jsonify({"error": "Thiếu username hoặc permission"}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT project_permission FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "Người dùng không tồn tại"}), 404

        permissions = user['project_permission'].split(",") if user['project_permission'] else []

        if permission_to_remove not in permissions:
            return jsonify({"message": "Quyền không tồn tại trong danh sách"}), 200

        permissions = [p for p in permissions if p != permission_to_remove]
        permission_str = ",".join(permissions)

        cursor.execute("UPDATE users SET project_permission = %s WHERE username = %s", (permission_str, username))
        conn.commit()
        return jsonify({"message": "Xoá quyền thành công!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
