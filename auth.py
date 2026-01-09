from flask import Blueprint, request, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from database import get_connection

auth_bp = Blueprint('auth', __name__)


def _pick_managed_root_unit_id(cursor, managed_unit_ids):
    """
    Chọn managed_organization_unit_id theo hướng:
    - Nếu user quản lý nhiều unit cùng bậc (2+ vị trí), trả về cấp CAO HƠN (tổ tiên chung gần nhất - LCA).
    - Nếu user chỉ quản lý 1 unit, trả về chính unit đó.
    - Nếu không có managed_unit_ids, trả về None.

    Lưu ý: chỉ sửa logic chọn managed_organization_unit_id, giữ nguyên các logic khác.
    """
    if not managed_unit_ids:
        return None

    # Lấy map id -> parent_id cho toàn bộ cây để có thể đi ngược lên ancestor
    cursor.execute("SELECT id, parent_id FROM organization_units")
    all_units = cursor.fetchall() or []
    parent_map = {u["id"]: u["parent_id"] for u in all_units}

    def get_ancestor_chain(start_id):
        """Trả về list tổ tiên từ node lên root: [start, parent, grandparent, ...]"""
        chain = []
        cur = start_id
        seen = set()
        while cur is not None and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            cur = parent_map.get(cur)
        return chain

    # Lấy chain tổ tiên cho từng managed unit
    chains = [get_ancestor_chain(mid) for mid in managed_unit_ids if mid is not None]
    if not chains:
        return None

    # Tổ tiên chung (intersection)
    common = set(chains[0])
    for ch in chains[1:]:
        common &= set(ch)

    if not common:
        # Dữ liệu parent_id có thể bị đứt/không liên thông; fallback ổn định
        return min(managed_unit_ids)

    # Chọn LCA: node chung có "tổng khoảng cách" nhỏ nhất tới các managed units
    # (node càng gần các managed units thì index trong chain càng nhỏ)
    def score(node_id):
        s = 0
        for ch in chains:
            try:
                s += ch.index(node_id)
            except ValueError:
                s += 10**9
        return s

    # min theo score, nếu hoà thì lấy id nhỏ hơn để ổn định
    return min(common, key=lambda nid: (score(nid), nid))


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

        # 6. ✅ SỬA LOGIC: managed_organization_unit_id là cấp CAO HƠN (tổ tiên chung gần nhất)
        managed_unit_id = _pick_managed_root_unit_id(cursor, managed_unit_ids)

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
                'managed_organization_unit_id': managed_unit_id,              # 👈 duy nhất (đã sửa: cấp cao hơn)
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


@auth_bp.route('/api/change-password', methods=['POST'])
def change_password():
    """
    Body JSON expected:
    {
      "username": "00001",
      "old_password": "current_plain_text_password",
      "new_password": "new_plain_text_password"
    }
    """
    data = request.get_json() or {}
    username = data.get('username')
    old_password = data.get('old_password')
    new_password = data.get('new_password')

    if not username or not old_password or not new_password:
        return jsonify({'error': 'username, old_password và new_password là bắt buộc'}), 400

    # ✅ Chỉ cần tối thiểu 4 ký tự
    if len(new_password) < 4:
        return jsonify({'error': 'Mật khẩu mới phải có ít nhất 4 ký tự'}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Kiểm tra user tồn tại
        cursor.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        if not user:
            return jsonify({'error': 'Tài khoản không tồn tại'}), 404

        # Kiểm tra mật khẩu cũ
        stored_hash = user.get('password_hash')
        if not stored_hash or not check_password_hash(stored_hash, old_password):
            return jsonify({'error': 'Mật khẩu hiện tại không đúng'}), 401

        # Tạo hash mới (dùng scrypt để giữ format giống cũ)
        new_hash = generate_password_hash(new_password, method='scrypt')

        # ✅ Cập nhật mật khẩu (không có updated_at)
        cursor.execute(
            "UPDATE users SET password_hash = %s WHERE id = %s",
            (new_hash, user['id'])
        )
        conn.commit()

        return jsonify({'message': 'Đổi mật khẩu thành công'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


@auth_bp.route('/api/admin/reset-password', methods=['POST'])
def admin_reset_password():
    """
    Body JSON expected:
    {
      "employee_id": 123,
      "new_password": "new_plain_text_password"
    }
    """
    data = request.get_json() or {}
    employee_id = data.get('employee_id')
    new_password = data.get('new_password')

    # ✅ employee_id bắt buộc, new_password chỉ cần không rỗng
    if not employee_id:
        return jsonify({'error': 'employee_id là bắt buộc'}), 400

    if new_password is None or str(new_password).strip() == "":
        return jsonify({'error': 'new_password là bắt buộc'}), 400

    # ✅ Cho phép 1 ký tự trở lên (không check len >= 4 nữa)
    new_password = str(new_password)

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # 1) Tìm employee_code từ employees2026
        cursor.execute("""
            SELECT e.id AS employee_id, e.employee_code
            FROM employees2026 e
            WHERE e.id = %s
            LIMIT 1
        """, (employee_id,))
        emp = cursor.fetchone()
        if not emp or not emp.get('employee_code'):
            return jsonify({'error': 'Không tìm thấy nhân viên hoặc thiếu employee_code'}), 404

        employee_code = emp['employee_code']

        # 2) Tìm user tương ứng trong bảng users (username = employee_code)
        cursor.execute("""
            SELECT u.id, u.username
            FROM users u
            WHERE u.username = %s
            LIMIT 1
        """, (employee_code,))
        user = cursor.fetchone()
        if not user:
            return jsonify({'error': 'Không tìm thấy tài khoản users tương ứng với employee_code'}), 404

        # 3) Tạo password hash mới (giữ method='scrypt' như bạn đang dùng)
        new_hash = generate_password_hash(new_password, method='scrypt')

        # 4) Cập nhật users.password_hash
        cursor.execute("""
            UPDATE users
            SET password_hash = %s
            WHERE id = %s
        """, (new_hash, user['id']))
        conn.commit()

        return jsonify({
            'message': 'Reset mật khẩu thành công',
            'username': user['username'],
            'employee_id': emp['employee_id']
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
@auth_bp.route('/api/admin/create-user', methods=['POST'])
def admin_create_user():
    """
    Tạo tài khoản đăng nhập trong bảng users.
    - username: bắt buộc (thường = employee_code)
    - password mặc định: "1"
    - hash dùng method='scrypt' giống change/reset password

    Body JSON expected:
    {
      "username": "00001"
    }
    """
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()

    if not username:
        return jsonify({"error": "username là bắt buộc"}), 400

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # (Khuyến nghị) Check employee_code có tồn tại trong employees2026
        cursor.execute("""
            SELECT id
            FROM employees2026
            WHERE employee_code = %s
            LIMIT 1
        """, (username,))
        emp = cursor.fetchone()
        if not emp:
            return jsonify({"error": "Không tìm thấy nhân viên với employee_code tương ứng"}), 404

        # Check username đã tồn tại trong users chưa
        cursor.execute("""
            SELECT id
            FROM users
            WHERE username = %s
            LIMIT 1
        """, (username,))
        existed = cursor.fetchone()
        if existed:
            return jsonify({"error": "Tài khoản đã tồn tại", "username": username}), 409

        # Mật khẩu mặc định = "1"
        default_password = "1"
        password_hash = generate_password_hash(default_password, method="scrypt")

        # Tạo user (tuỳ schema users của bạn có cột gì thêm thì bổ sung)
        cursor.execute("""
            INSERT INTO users (username, password_hash)
            VALUES (%s, %s)
        """, (username, password_hash))
        conn.commit()

        return jsonify({
            "message": "Tạo tài khoản thành công",
            "username": username,
            "default_password": "1"
        }), 201

    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
