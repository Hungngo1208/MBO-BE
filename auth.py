from flask import Blueprint, request, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from database import get_connection

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')  # employee_code
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': 'Username and password are required'}), 400

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # 1. Kiểm tra tài khoản trong bảng users
        cursor.execute("SELECT username, password_hash FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()

        if not user or not check_password_hash(user['password_hash'], password):
            return jsonify({'error': 'Invalid username or password'}), 401

        # 2. Lấy thông tin nhân sự
        cursor.execute("""
            SELECT id, entry_date, full_name, gender, employee_code, birth_date, phone,
                   position, corporation, company, factory, division, sub_division, section,
                   group_name, note, organization_unit_id
            FROM employees2026
            WHERE employee_code = %s
        """, (username,))
        employee = cursor.fetchone()

        if not employee:
            return jsonify({'error': 'Employee profile not found'}), 404

        employee_id = employee['id']

        # 3. Lấy vai trò
        cursor.execute("""
            SELECT r.name
            FROM employee_roles er
            JOIN roles r ON er.role_id = r.id
            WHERE er.employee_id = %s
        """, (employee_id,))
        roles = [row['name'] for row in cursor.fetchall()]

        # 4. Lấy quyền
        cursor.execute("""
            SELECT DISTINCT p.code
            FROM employee_roles er
            JOIN role_permissions rp ON er.role_id = rp.role_id
            JOIN permissions p ON rp.permission_id = p.id
            WHERE er.employee_id = %s
        """, (employee_id,))
        permissions = [row['code'] for row in cursor.fetchall()]

        # 5. Lấy tất cả phòng ban người này quản lý
        cursor.execute("""
            SELECT id, parent_id FROM organization_units
            WHERE employee_id = %s
        """, (employee_id,))
        units = cursor.fetchall()
        managed_unit_ids = [u['id'] for u in units]

        # 6. Lọc ra cấp cao nhất (không có cha nằm trong danh sách đang quản lý)
        top_level_units = [
            u['id'] for u in units
            if u['parent_id'] not in managed_unit_ids
        ]
        managed_unit_id = top_level_units[0] if top_level_units else None  # chọn 1 cái duy nhất

        # 7. Tạo token giả (hoặc dùng JWT nếu có)
        token = generate_password_hash(username)[:32]

        # 8. Trả kết quả
        return jsonify({
            'message': 'Login successful',
            'token': token,
            'user': {
                'id': employee['id'],
                'employee_code': employee['employee_code'],
                'full_name': employee['full_name'],
                'gender': employee['gender'],
                'birth_date': str(employee['birth_date']),
                'entry_date': str(employee['entry_date']),
                'phone': employee['phone'],
                'position': employee['position'],
                'corporation': employee['corporation'],
                'company': employee['company'],
                'factory': employee['factory'],
                'division': employee['division'],
                'sub_division': employee['sub_division'],
                'section': employee['section'],
                'group_name': employee['group_name'],
                'note': employee['note'],
                'organization_unit_id': employee['organization_unit_id'],
                'roles': roles,
                'permissions': permissions,
                'managed_organization_unit_id': managed_unit_id,              # 👈 duy nhất
                'managed_organization_unit_ids': managed_unit_ids             # 👈 đầy đủ (tuỳ dùng)
            }
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
