from flask import Blueprint, jsonify, request
from database import get_connection
from datetime import date, datetime

employees_bp = Blueprint('employees', __name__, url_prefix='/employees')

# ======= Hàm phụ trợ =======
def get_parent_map():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, parent_id FROM organization_units")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {row["id"]: row["parent_id"] for row in rows}

def get_all_parents(unit_id, parent_map):
    result = []
    while unit_id:
        result.append(unit_id)
        unit_id = parent_map.get(unit_id)
    return result

def update_employee_count(unit_id, delta):
    if not unit_id:
        return
    parent_map = get_parent_map()
    conn = get_connection()
    cursor = conn.cursor()
    for uid in get_all_parents(unit_id, parent_map):
        cursor.execute("""
            UPDATE organization_units
            SET employee_count = employee_count + %s
            WHERE id = %s
        """, (delta, uid))
    conn.commit()
    cursor.close()
    conn.close()

# ======= API =======
# GET - Lấy danh sách nhân viên (có thể lọc theo phòng ban cha + phòng con)
EMPLOYEE_TABLE = "`db qlda`.employees2026_base"  # bảng thật sau khi rename
@employees_bp.route('/list', methods=['GET'])
def get_employees_list():
    from datetime import date, datetime
    from flask import request, jsonify

    org_id = request.args.get('org_id', type=int)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    base_select = f"""
        SELECT e.id, e.entry_date, e.full_name, e.gender, e.employee_code,
               e.birth_date, e.phone, e.position, e.corporation, e.company,
               e.factory, e.division, e.sub_division, e.section, e.group_name,
               e.note, e.organization_unit_id,
               e.employment_status,        -- NEW
               e.status_note,              -- NEW
               ou.name AS organization_unit_name
        FROM {EMPLOYEE_TABLE} e
        LEFT JOIN organization_units ou ON e.organization_unit_id = ou.id
    """

    if org_id:
        # Lấy tất cả phòng ban con (CTE)
        cursor.execute("""
            WITH RECURSIVE org_tree AS (
                SELECT id, parent_id, name
                FROM organization_units
                WHERE id = %s
                UNION ALL
                SELECT ou.id, ou.parent_id, ou.name
                FROM organization_units ou
                INNER JOIN org_tree ot ON ou.parent_id = ot.id
            )
            SELECT id FROM org_tree
        """, (org_id,))
        descendant_ids = [r["id"] for r in cursor.fetchall()]

        if not descendant_ids:
            cursor.close()
            conn.close()
            return jsonify([])

        format_strings = ",".join(["%s"] * len(descendant_ids))
        query = base_select + f" WHERE e.organization_unit_id IN ({format_strings})"
        cursor.execute(query, descendant_ids)
    else:
        cursor.execute(base_select)

    rows = cursor.fetchall()

    # Chuẩn hóa định dạng ngày
    for row in rows:
        for date_field in ['entry_date', 'birth_date']:
            if isinstance(row.get(date_field), (date, datetime)):
                row[date_field] = row[date_field].isoformat()

    cursor.close()
    conn.close()
    return jsonify(rows)
@employees_bp.route('/add', methods=['POST'])
def add_employee():
    from flask import request, jsonify
    data = request.json or {}

    conn = get_connection()
    cursor = conn.cursor()

    # Giá trị mặc định nếu client không truyền
    employment_status = data.get('employment_status') or 'active'
    status_note = data.get('status_note')

    cursor.execute(f"""
        INSERT INTO {EMPLOYEE_TABLE} (
            entry_date, full_name, gender, employee_code, birth_date, phone,
            position, corporation, company, factory, division,
            sub_division, section, group_name, note, organization_unit_id,
            employment_status, status_note             -- NEW
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data.get('entry_date'),
        data.get('full_name'),
        data.get('gender'),
        data.get('employee_code'),
        data.get('birth_date'),
        data.get('phone'),
        data.get('position'),
        data.get('corporation'),
        data.get('company'),
        data.get('factory'),
        data.get('division'),
        data.get('sub_division'),
        data.get('section'),
        data.get('group_name'),
        data.get('note'),
        data.get('organization_unit_id'),
        employment_status,    # NEW
        status_note           # NEW
    ))

    # Cập nhật employee_count nếu có organization_unit_id và trạng thái là active
    if data.get('organization_unit_id') and employment_status == 'active':
        update_employee_count(data['organization_unit_id'], +1)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"message": "Thêm nhân viên thành công"}), 201
@employees_bp.route('/update/<int:id>', methods=['PUT'])
def update_employee(id):
    from flask import request, jsonify
    data = request.json or {}

    conn = get_connection()
    cursor = conn.cursor()

    # Lấy hiện trạng
    cursor.execute(f"SELECT * FROM {EMPLOYEE_TABLE} WHERE id = %s", (id,))
    current = cursor.fetchone()
    if not current:
        return jsonify({"error": "Không tìm thấy nhân viên"}), 404

    colnames = [desc[0] for desc in cursor.description]
    current_data = dict(zip(colnames, current))

    old_unit_id = current_data.get('organization_unit_id')
    new_unit_id = data.get('organization_unit_id', old_unit_id)

    old_status = current_data.get('employment_status') or 'active'
    new_status = data.get('employment_status', old_status)

    # Các trường chung
    general_fields = [
        'entry_date', 'full_name', 'gender', 'employee_code', 'birth_date',
        'phone', 'position', 'note'
    ]
    department_fields = [
        'corporation', 'company', 'factory', 'division',
        'sub_division', 'section', 'group_name'
    ]

    update_values = []
    for field in general_fields:
        update_values.append(data.get(field, current_data[field]))
    for field in department_fields:
        update_values.append(data.get(field, current_data[field]))

    # NEW: thêm 2 field mới
    status_note = data.get('status_note', current_data.get('status_note'))
    update_values.extend([new_unit_id, new_status, status_note])

    cursor.execute(f"""
        UPDATE {EMPLOYEE_TABLE} SET
            entry_date=%s,
            full_name=%s,
            gender=%s,
            employee_code=%s,
            birth_date=%s,
            phone=%s,
            position=%s,
            note=%s,
            corporation=%s,
            company=%s,
            factory=%s,
            division=%s,
            sub_division=%s,
            section=%s,
            group_name=%s,
            organization_unit_id=%s,
            employment_status=%s,     -- NEW
            status_note=%s            -- NEW
        WHERE id=%s
    """, (*update_values, id))

    # Cập nhật employee_count khi đổi phòng ban
    if old_unit_id != new_unit_id:
        if old_unit_id and old_status == 'active':
            update_employee_count(old_unit_id, -1)
        if new_unit_id and new_status == 'active':
            update_employee_count(new_unit_id, +1)
    else:
        # Không đổi phòng ban, nhưng có thể đổi trạng thái
        if old_status != new_status and new_unit_id:
            if old_status == 'active' and new_status == 'terminated':
                update_employee_count(new_unit_id, -1)
            elif old_status == 'terminated' and new_status == 'active':
                update_employee_count(new_unit_id, +1)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"message": "Cập nhật thành công"})

# DELETE - Xoá nhân viên
@employees_bp.route('/delete/<int:id>', methods=['DELETE'])
def delete_employee(id):
    from flask import jsonify

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Lấy unit & trạng thái từ bảng GỐC
        cursor.execute("""
            SELECT organization_unit_id, employment_status
            FROM `db qlda`.employees2026_base
            WHERE id = %s
        """, (id,))
        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return jsonify({"error": "Không tìm thấy nhân viên"}), 404

        org_unit_id, employment_status = row[0], row[1]

        # Nếu đang active thì trừ số lượng trước khi xoá
        if org_unit_id and (employment_status or 'active') == 'active':
            update_employee_count(org_unit_id, -1)

        # Xoá bản ghi ở bảng GỐC
        cursor.execute("""
            DELETE FROM `db qlda`.employees2026_base
            WHERE id = %s
        """, (id,))

        conn.commit()
        return jsonify({"message": "Xoá thành công"})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@employees_bp.route('/by-department/<int:unit_id>', methods=['GET'])
def get_employees_by_department(unit_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        WITH RECURSIVE descendants AS (
            SELECT id FROM organization_units WHERE id = %s
            UNION ALL
            SELECT o.id
            FROM organization_units o
            JOIN descendants d ON o.parent_id = d.id
        )
        SELECT 
            e.id,
            e.full_name,
            e.employee_code,
            e.position,
            e.phone,
            e.entry_date,
            e.birth_date,
            e.gender,
            e.note,
            e.corporation,
            e.company,
            e.factory,
            e.division,
            e.sub_division,
            e.section,
            e.group_name,
            e.organization_unit_id,
            COALESCE(ms.status, 'draft') AS status,
            ms.reviewer_id,
            ms.approver_id,

            -- Điểm công việc (approved_ey_score * approver_ti_trong / 100)
            (
                SELECT COALESCE(
                    SUM(ROUND(
                        (COALESCE(p.approved_ey_score, 0) * COALESCE(p.approver_ti_trong, 0)) / 100
                    , 2)), 0
                )
                FROM `db qlda`.personalmbo p
                WHERE p.employee_code = e.employee_code
                  AND p.mbo_year = YEAR(CURDATE())
            ) AS job_score,

            -- Điểm năng lực (approved_ey_score * approver_ti_trong / 100)
            (
                SELECT COALESCE(
                    SUM(ROUND(
                        (COALESCE(c.approved_ey_score, 0) * COALESCE(c.approver_ti_trong, 0)) / 100
                    , 2)), 0
                )
                FROM `db qlda`.competencymbo c
                WHERE c.employee_code = e.employee_code
                  AND c.mbo_year = YEAR(CURDATE())
            ) AS competency_score,

            -- score_final: ưu tiên lấy từ ms.score_final; nếu NULL thì trung bình(job_score, competency_score)
            COALESCE(
                ms.score_final,
                (
                    (
                        SELECT COALESCE(
                            SUM(ROUND(
                                (COALESCE(p2.approved_ey_score, 0) * COALESCE(p2.approver_ti_trong, 0)) / 100
                            , 2)), 0
                        )
                        FROM `db qlda`.personalmbo p2
                        WHERE p2.employee_code = e.employee_code
                          AND p2.mbo_year = YEAR(CURDATE())
                    )
                    +
                    (
                        SELECT COALESCE(
                            SUM(ROUND(
                                (COALESCE(c2.approved_ey_score, 0) * COALESCE(c2.approver_ti_trong, 0)) / 100
                            , 2)), 0
                        )
                        FROM `db qlda`.competencymbo c2
                        WHERE c2.employee_code = e.employee_code
                          AND c2.mbo_year = YEAR(CURDATE())
                    )
                ) / 2
            ) AS score_final,

            CASE
                WHEN e.group_name IS NOT NULL AND e.group_name != '' THEN e.group_name
                WHEN e.section IS NOT NULL AND e.section != '' THEN e.section
                WHEN e.sub_division IS NOT NULL AND e.sub_division != '' THEN e.sub_division
                WHEN e.division IS NOT NULL AND e.division != '' THEN e.division
                WHEN e.factory IS NOT NULL AND e.factory != '' THEN e.factory
                WHEN e.company IS NOT NULL AND e.company != '' THEN e.company
                WHEN e.corporation IS NOT NULL AND e.corporation != '' THEN e.corporation
                ELSE NULL
            END AS department_name
        FROM `db qlda`.employees2026 e
        JOIN descendants d ON e.organization_unit_id = d.id
        LEFT JOIN `db qlda`.mbo_sessions ms 
            ON e.id = ms.employee_id AND ms.mbo_year = YEAR(CURDATE())
    """

    cursor.execute(query, (unit_id,))
    employees = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(employees)





@employees_bp.route("/accessible-units", methods=["GET"])
def get_accessible_organization_units():
    from flask import request, jsonify
    from database import get_connection

    org_unit_id = request.headers.get("X-Org-Unit-Id")
    permissions = request.headers.get("X-Permissions", "")

    # Parse quyền
    try:
        permissions_list = [p.strip() for p in permissions.split(",") if p.strip()]
    except:
        permissions_list = []

    connection = get_connection()
    try:
        with connection.cursor(dictionary=True) as cursor:
            # ✅ Nếu có quyền cao nhất thì trả toàn bộ cây
            if "view_FY_review" in permissions_list:
                cursor.execute("SELECT * FROM organization_units")
                return jsonify(cursor.fetchall())

            # ✅ Nếu không có quyền thì cần org_unit_id
            if not org_unit_id or org_unit_id == "null":
                return jsonify([])  # Trả mảng rỗng nếu không có quyền truy cập

            try:
                org_unit_id = int(org_unit_id)
            except ValueError:
                return jsonify({"error": "Invalid organization unit ID"}), 400

            # ✅ Truy vấn đệ quy để lấy các phòng ban con
            query = """
                WITH RECURSIVE sub_units AS (
                    SELECT id, name, type, parent_id
                    FROM organization_units
                    WHERE id = %s
                    UNION ALL
                    SELECT ou.id, ou.name, ou.type, ou.parent_id
                    FROM organization_units ou
                    JOIN sub_units su ON ou.parent_id = su.id
                )
                SELECT * FROM sub_units;
            """
            cursor.execute(query, (org_unit_id,))
            return jsonify(cursor.fetchall())

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        connection.close()


def get_all_sub_unit_ids(start_ids, cursor):
    """
    Đệ quy lấy tất cả organization_unit_id con của các ID được truyền vào
    """
    all_ids = set(start_ids)
    queue = list(start_ids)

    while queue:
        current_id = queue.pop(0)
        cursor.execute("SELECT id FROM organization_units WHERE parent_id = %s", (current_id,))
        children = [row["id"] for row in cursor.fetchall()]
        queue.extend(children)
        all_ids.update(children)

    return list(all_ids)


@employees_bp.route('/by-subordinates', methods=['POST'])
def get_employees_for_allocation():
    data = request.get_json()
    managed_ids = data.get("managed_organization_unit_ids")
    current_user_id = data.get("current_user_id")

    if not managed_ids or not isinstance(managed_ids, list):
        return jsonify({"error": "Thiếu managed_organization_unit_ids"}), 400

    if not current_user_id:
        return jsonify({"error": "Thiếu current_user_id"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        result_employees = []

        def find_first_managers_recursively(unit_id):
            """
            Tìm người quản lý cấp dưới đầu tiên của unit_id (nếu unit đó không có người quản lý trực tiếp).
            Dừng ngay khi tìm thấy cấp có người quản lý.
            """
            cursor.execute("""
                SELECT id, employee_id
                FROM organization_units
                WHERE parent_id = %s
            """, (unit_id,))
            children = cursor.fetchall()

            # Lọc những đơn vị con có người quản lý (khác current_user_id)
            managed_emp_ids = [row["employee_id"] for row in children if row["employee_id"] and row["employee_id"] != current_user_id]

            if managed_emp_ids:
                # Nếu tìm được ít nhất một người quản lý ở cấp này, trả về luôn
                return managed_emp_ids
            else:
                # Không có người quản lý ở cấp này → tiếp tục tìm cấp con
                collected = []
                for child in children:
                    collected += find_first_managers_recursively(child["id"])
                return collected

        for managed_id in managed_ids:
            # 1. Tìm đơn vị con trực tiếp
            cursor.execute("""
                SELECT id, employee_id
                FROM organization_units
                WHERE parent_id = %s
            """, (managed_id,))
            children = cursor.fetchall()

            # 2. Tìm người quản lý trực tiếp cấp dưới (nếu có)
            emp_ids = [row["employee_id"] for row in children if row["employee_id"] and row["employee_id"] != current_user_id]

            if emp_ids:
                emp_format = ','.join(['%s'] * len(emp_ids))
                cursor.execute(
                    f"SELECT * FROM employees2026 WHERE id IN ({emp_format})",
                    tuple(emp_ids)
                )
                result_employees.extend(cursor.fetchall())
            else:
                # ❌ Không có người quản lý trực tiếp ở cấp dưới → tìm đệ quy
                recursive_emp_ids = find_first_managers_recursively(managed_id)

                if recursive_emp_ids:
                    emp_format = ','.join(['%s'] * len(recursive_emp_ids))
                    cursor.execute(
                        f"SELECT * FROM employees2026 WHERE id IN ({emp_format})",
                        tuple(recursive_emp_ids)
                    )
                    result_employees.extend(cursor.fetchall())
                else:
                    # ✅ Không có quản lý cấp dưới nào → lấy nhân viên trong đơn vị đó
                    cursor.execute("""
                        SELECT *
                        FROM employees2026
                        WHERE organization_unit_id = %s AND id != %s
                    """, (managed_id, current_user_id))
                    result_employees.extend(cursor.fetchall())

        # Loại trùng và định dạng ngày
        seen = set()
        final_result = []
        for emp in result_employees:
            if emp['id'] not in seen:
                seen.add(emp['id'])
                for field in ['entry_date', 'birth_date']:
                    if isinstance(emp.get(field), (date, datetime)):
                        emp[field] = emp[field].isoformat()
                final_result.append(emp)

        return jsonify(final_result)

    finally:
        cursor.close()
        conn.close()
@employees_bp.route('/mbo/score-final', methods=['PUT'])
def update_score_final():
    """
    Cập nhật trực tiếp score_final trong bảng `mbo_sessions`
    theo employee_id và mbo_year.
    Body JSON bắt buộc:
      {
        "employee_id": 1425,
        "mbo_year": 2025,
        "score_final": 95.5
      }
    """
    data = request.get_json(silent=True) or {}

    employee_id = data.get("employee_id")
    mbo_year = data.get("mbo_year")
    score_final = data.get("score_final")

    if not employee_id or not mbo_year or score_final is None:
        return jsonify({"error": "Missing employee_id, mbo_year hoặc score_final"}), 400

    try:
        score_final = round(float(score_final), 2)
    except (ValueError, TypeError):
        return jsonify({"error": "score_final phải là số"}), 400

    if score_final < 0 or score_final > 100:
        return jsonify({"error": "score_final phải nằm trong khoảng 0–100"}), 400

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE `db qlda`.mbo_sessions
            SET score_final = %s
            WHERE employee_id = %s AND mbo_year = %s
        """, (score_final, employee_id, mbo_year))
        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"error": "Không tìm thấy session với employee_id và mbo_year này"}), 404

        return jsonify({
            "employee_id": employee_id,
            "mbo_year": mbo_year,
            "score_final": score_final,
            "message": "score_final updated thành công"
        })
    finally:
        cursor.close()
        conn.close()



