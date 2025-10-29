from flask import Blueprint, jsonify, request
from database import get_connection, DB_SCHEMA
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
EMPLOYEE_TABLE = f"`{DB_SCHEMA}`.employees2026_base"  # bảng thật sau khi rename

# GET - Lấy danh sách nhân viên
@employees_bp.route('/list', methods=['GET'])
def get_employees_list():
    org_id = request.args.get('org_id', type=int)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    base_select = f"""
        SELECT e.id, e.entry_date, e.full_name, e.gender, e.employee_code,
               e.birth_date, e.phone, e.position, e.cap_bac,
               e.corporation, e.company, e.factory, e.division, e.sub_division,
               e.section, e.group_name, e.note, e.organization_unit_id,
               e.employment_status, e.status_note,
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

# POST - Thêm nhân viên
@employees_bp.route('/add', methods=['POST'])
def add_employee():
    data = request.json or {}

    conn = get_connection()
    cursor = conn.cursor()

    employment_status = data.get('employment_status') or 'active'
    status_note = data.get('status_note')

    cursor.execute(f"""
        INSERT INTO {EMPLOYEE_TABLE} (
            entry_date, full_name, gender, employee_code, birth_date, phone,
            position, cap_bac,
            corporation, company, factory, division,
            sub_division, section, group_name, note, organization_unit_id,
            employment_status, status_note
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
    """, (
        data.get('entry_date'),
        data.get('full_name'),
        data.get('gender'),
        data.get('employee_code'),
        data.get('birth_date'),
        data.get('phone'),
        data.get('position'),
        data.get('cap_bac'),
        data.get('corporation'),
        data.get('company'),
        data.get('factory'),
        data.get('division'),
        data.get('sub_division'),
        data.get('section'),
        data.get('group_name'),
        data.get('note'),
        data.get('organization_unit_id'),
        employment_status,
        status_note
    ))

    if data.get('organization_unit_id') and employment_status == 'active':
        update_employee_count(data['organization_unit_id'], +1)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"message": "Thêm nhân viên thành công"}), 201

# PUT - Cập nhật nhân viên
@employees_bp.route('/update/<int:id>', methods=['PUT'])
def update_employee(id):
    data = request.json or {}

    conn = get_connection()
    cursor = conn.cursor()

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

    # Trường chung
    general_fields = [
        'entry_date', 'full_name', 'gender', 'employee_code', 'birth_date',
        'phone', 'position', 'cap_bac', 'note'  # cap_bac trước note
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
            cap_bac=%s,
            note=%s,
            corporation=%s,
            company=%s,
            factory=%s,
            division=%s,
            sub_division=%s,
            section=%s,
            group_name=%s,
            organization_unit_id=%s,
            employment_status=%s,
            status_note=%s
        WHERE id=%s
    """, (*update_values, id))

    # Cập nhật employee_count
    if old_unit_id != new_unit_id:
        if old_unit_id and old_status == 'active':
            update_employee_count(old_unit_id, -1)
        if new_unit_id and new_status == 'active':
            update_employee_count(new_unit_id, +1)
    else:
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
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            SELECT organization_unit_id, employment_status
            FROM `{DB_SCHEMA}`.employees2026_base
            WHERE id = %s
        """, (id,))
        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return jsonify({"error": "Không tìm thấy nhân viên"}), 404

        org_unit_id, employment_status = row[0], row[1]

        if org_unit_id and (employment_status or 'active') == 'active':
            update_employee_count(org_unit_id, -1)

        cursor.execute(f"""
            DELETE FROM `{DB_SCHEMA}`.employees2026_base
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
    from datetime import date
    mbo_year = request.args.get('mbo_year', type=int) or date.today().year

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")

    query = f"""
        WITH RECURSIVE descendants AS (
            SELECT id FROM organization_units WHERE id = %s
            UNION ALL
            SELECT o.id
            FROM organization_units o
            JOIN descendants d ON o.parent_id = d.id
        )
        /* Lớp 1: tính sẵn các điểm REVIEWED/APPROVED + thái độ */
        , base AS (
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

                COALESCE(ms.status, 'draft')       AS status,
                ms.reviewer_id,
                ms.approver_id,
                ms.score_final                     AS ms_score_final,
                COALESCE(ms.attitude_status,'none') AS attitude_status,

                /* Điểm công việc (REVIEWED) */
                (
                  SELECT COALESCE(
                    SUM(ROUND(
                      (COALESCE(p.reviewed_ey_score, 0) * COALESCE(p.reviewer_ti_trong, 0)) / 100
                    , 2)), 0
                  )
                  FROM `{DB_SCHEMA}`.personalmbo p
                  WHERE p.employee_code COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
                    AND p.mbo_year = %s
                ) AS job_score_reviewed,

                /* Điểm công việc (APPROVED) */
                (
                  SELECT COALESCE(
                    SUM(ROUND(
                      (COALESCE(p.approved_ey_score, 0) * COALESCE(p.approver_ti_trong, 0)) / 100
                    , 2)), 0
                  )
                  FROM `{DB_SCHEMA}`.personalmbo p
                  WHERE p.employee_code COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
                    AND p.mbo_year = %s
                ) AS job_score_approved,

                /* Điểm năng lực (REVIEWED) */
                (
                  SELECT COALESCE(
                    SUM(ROUND(
                      (COALESCE(c.reviewed_ey_score, 0) * COALESCE(c.reviewer_ti_trong, 0)) / 100
                    , 2)), 0
                  )
                  FROM `{DB_SCHEMA}`.competencymbo c
                  WHERE c.employee_code COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
                    AND c.mbo_year = %s
                ) AS competency_score_reviewed,

                /* Điểm năng lực (APPROVED) */
                (
                  SELECT COALESCE(
                    SUM(ROUND(
                      (COALESCE(c.approved_ey_score, 0) * COALESCE(c.approver_ti_trong, 0)) / 100
                    , 2)), 0
                  )
                  FROM `{DB_SCHEMA}`.competencymbo c
                  WHERE c.employee_code COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
                    AND c.mbo_year = %s
                ) AS competency_score_approved,

                /* Điểm thái độ (AVG theo năm, dùng cho cả reviewed/approved) */
                (
                  SELECT COALESCE(ROUND(AVG(a.score), 2), 0)
                  FROM `{DB_SCHEMA}`.attitudembo a
                  WHERE a.employee_code COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
                    AND a.mbo_year = %s
                ) AS attitude_score_year,

                /* Tên phòng ban nhỏ nhất để hiển thị */
                CASE
                    WHEN e.group_name      IS NOT NULL AND e.group_name      != '' THEN e.group_name
                    WHEN e.section         IS NOT NULL AND e.section         != '' THEN e.section
                    WHEN e.sub_division    IS NOT NULL AND e.sub_division    != '' THEN e.sub_division
                    WHEN e.division        IS NOT NULL AND e.division        != '' THEN e.division
                    WHEN e.factory         IS NOT NULL AND e.factory         != '' THEN e.factory
                    WHEN e.company         IS NOT NULL AND e.company         != '' THEN e.company
                    WHEN e.corporation     IS NOT NULL AND e.corporation     != '' THEN e.corporation
                    ELSE NULL
                END AS department_name

            FROM `{DB_SCHEMA}`.employees2026 e
            JOIN descendants d ON e.organization_unit_id = d.id
            LEFT JOIN `{DB_SCHEMA}`.mbo_sessions ms
                ON e.id = ms.employee_id AND ms.mbo_year = %s
        )

        /* Lớp 2: chọn điểm theo status + tính computed_final */
        , prepared AS (
            SELECT
                b.*,

                /* job_score theo status */
                CASE
                  WHEN b.status = 'reviewed_final' THEN b.job_score_reviewed
                  WHEN b.status = 'approved_final' THEN b.job_score_approved
                  ELSE 0
                END AS job_score,

                /* competency_score theo status */
                CASE
                  WHEN b.status = 'reviewed_final' THEN b.competency_score_reviewed
                  WHEN b.status = 'approved_final' THEN b.competency_score_approved
                  ELSE 0
                END AS competency_score,

                /* attitude_score theo status (2 trạng thái dùng chung thái độ năm) */
                CASE
                  WHEN b.status IN ('reviewed_final','approved_final') THEN b.attitude_score_year
                  ELSE 0
                END AS attitude_score,

                /* computed_final theo position + status (các trạng thái khác = 0) */
                CASE
                  WHEN b.status NOT IN ('reviewed_final','approved_final') THEN 0
                  ELSE
                    CASE
                      /* TL/Trưởng nhóm/Phó nhóm */
                      WHEN  LOWER(b.position) REGEXP 'trưởng[[:space:]]*nhóm|truong[[:space:]]*nhom|phó[[:space:]]*nhóm|pho[[:space:]]*nhom|team[[:space:]]*lead'
                         OR  CONCAT(' ', LOWER(b.position), ' ') LIKE '% tl %'
                      THEN ROUND(
                           0.10 * b.attitude_score_year
                         + 0.45 * (CASE WHEN b.status='reviewed_final' THEN b.job_score_reviewed        ELSE b.job_score_approved        END)
                         + 0.45 * (CASE WHEN b.status='reviewed_final' THEN b.competency_score_reviewed ELSE b.competency_score_approved END)
                      , 2)

                      /* Nhân viên */
                      WHEN  LOWER(b.position) REGEXP 'nhân[[:space:]]*viên|nhan[[:space:]]*vien|staff|employee'
                      THEN ROUND(
                           0.20 * b.attitude_score_year
                         + 0.40 * (CASE WHEN b.status='reviewed_final' THEN b.job_score_reviewed        ELSE b.job_score_approved        END)
                         + 0.40 * (CASE WHEN b.status='reviewed_final' THEN b.competency_score_reviewed ELSE b.competency_score_approved END)
                      , 2)

                      /* Cấp khác: bỏ thái độ */
                      ELSE ROUND(
                           0.50 * (CASE WHEN b.status='reviewed_final' THEN b.job_score_reviewed        ELSE b.job_score_approved        END)
                         + 0.50 * (CASE WHEN b.status='reviewed_final' THEN b.competency_score_reviewed ELSE b.competency_score_approved END)
                      , 2)
                    END
                END AS computed_final
            FROM base b
        )

        /* Lớp 3: xuất dữ liệu cuối cùng + score_final theo yêu cầu */
        SELECT
            p.id,
            p.full_name,
            p.employee_code,
            p.position,
            p.phone,
            p.entry_date,
            p.birth_date,
            p.gender,
            p.note,
            p.corporation,
            p.company,
            p.factory,
            p.division,
            p.sub_division,
            p.section,
            p.group_name,
            p.organization_unit_id,

            p.status,
            p.reviewer_id,
            p.approver_id,

            p.job_score,
            p.competency_score,
            p.attitude_score,
            p.attitude_status,

            p.computed_final,

            /* score_final: chỉ cho reviewed/approved; ưu tiên ms_score_final nếu có */
            CASE
              WHEN p.ms_score_final IS NOT NULL THEN ROUND(p.ms_score_final, 2)
              WHEN p.status IN ('reviewed_final','approved_final') THEN p.computed_final
              ELSE 0
            END AS score_final,

            p.department_name
        FROM prepared p
    """

    try:
        # placeholders: unit_id + 5×mbo_year (job_r, job_a, comp_r, comp_a, attitude) + 1×mbo_year (join ms)
        cursor.execute(
            query,
            (
                unit_id,
                mbo_year,  # job_score_reviewed
                mbo_year,  # job_score_approved
                mbo_year,  # competency_score_reviewed
                mbo_year,  # competency_score_approved
                mbo_year,  # attitude_score_year
                mbo_year,  # join mbo_sessions
            ),
        )
        rows = cursor.fetchall()
        return jsonify(rows)
    finally:
        cursor.close()
        conn.close()



@employees_bp.route("/accessible-units", methods=["GET"])
def get_accessible_organization_units():
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

@employees_bp.route("/accessible-units-v2", methods=["GET"])
def get_accessible_organization_units_v2():
    """
    Trả về tập unit người dùng có thể thấy:
    - Nếu có quyền 'view_FY_review' => trả toàn bộ organization_units
    - Ngược lại: UNION giữa tất cả CON (descendants, gồm chính nó)
                 và tất cả CHA (ancestors, gồm chính nó) của X-Org-Unit-Id
    Headers:
      - X-Org-Unit-Id: id phòng ban đang quản lý
      - X-Permissions: chuỗi quyền, ví dụ: "view_FY_review,edit_employee"
    """
    org_unit_id = request.headers.get("X-Org-Unit-Id")
    permissions = request.headers.get("X-Permissions", "")

    # Parse quyền
    try:
        permissions_list = [p.strip() for p in permissions.split(",") if p.strip()]
    except Exception:
        permissions_list = []

    # Nếu có quyền cao nhất => trả toàn bộ
    connection = get_connection()
    try:
        with connection.cursor(dictionary=True) as cursor:
            if "view_FY_review" in permissions_list:
                cursor.execute("""
                    SELECT id, name, type, parent_id
                    FROM organization_units
                """)
                return jsonify(cursor.fetchall())

            # Không có quyền cao: cần org_unit_id hợp lệ
            if not org_unit_id or org_unit_id == "null":
                return jsonify([])

            try:
                managed_id = int(org_unit_id)
            except ValueError:
                return jsonify({"error": "Invalid organization unit ID"}), 400

            # CTE đệ quy: lấy descendants và ancestors, rồi UNION DISTINCT
            query = """
                WITH RECURSIVE
                desc_units AS (
                    SELECT id, name, type, parent_id
                    FROM organization_units
                    WHERE id = %s
                    UNION ALL
                    SELECT ou.id, ou.name, ou.type, ou.parent_id
                    FROM organization_units ou
                    JOIN desc_units d ON ou.parent_id = d.id
                ),
                anc_units AS (
                    SELECT id, name, type, parent_id
                    FROM organization_units
                    WHERE id = %s
                    UNION ALL
                    SELECT p.id, p.name, p.type, p.parent_id
                    FROM organization_units p
                    JOIN anc_units a ON a.parent_id = p.id
                )
                SELECT DISTINCT id, name, type, parent_id
                FROM (
                    SELECT * FROM desc_units
                    UNION ALL
                    SELECT * FROM anc_units
                ) u
                ORDER BY id;
            """
            cursor.execute(query, (managed_id, managed_id))
            data = cursor.fetchall()
            return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            connection.close()
        except Exception:
            pass

@employees_bp.route('/by-subordinates', methods=['POST'])
def get_employees_for_allocation():
    from datetime import date, datetime

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
        # 1) Preload toàn bộ organization_units
        cursor.execute("""
            SELECT id, parent_id, employee_id
            FROM organization_units
        """)
        units = cursor.fetchall()

        # Build maps
        children_map = {}
        unit_by_id = {}
        for u in units:
            unit_by_id[u["id"]] = u
            children_map.setdefault(u["parent_id"], []).append(u)

        def find_first_managers_bfs(start_unit_id, current_user_id):
            """
            BFS từ start_unit_id để tìm 'độ sâu đầu tiên' có ít nhất 1 unit có employee_id (≠ current_user_id).
            Trả về: set(employee_ids) nếu tìm được; ngược lại trả về set() (không có quản lý trong toàn subtree).
            """
            from collections import deque

            visited = set()
            q = deque()
            q.append(start_unit_id)
            visited.add(start_unit_id)

            while q:
                level_size = len(q)
                found_managers = set()

                # Duyệt từng level một
                for _ in range(level_size):
                    uid = q.popleft()
                    u = unit_by_id.get(uid)
                    # Kiểm tra quản lý ở node hiện tại
                    if u and u.get("employee_id") and u["employee_id"] != current_user_id:
                        found_managers.add(u["employee_id"])
                    # Thêm con vào hàng đợi
                    for child in children_map.get(uid, []):
                        cid = child["id"]
                        if cid not in visited:
                            visited.add(cid)
                            q.append(cid)

                # Nếu level này có >=1 quản lý → trả về luôn (độ sâu đầu tiên)
                if found_managers:
                    return found_managers

            # Duyệt hết mà không thấy quản lý
            return set()

        def collect_leaf_units(start_unit_id):
            """
            Thu thập tất cả unit lá (không có con) trong subtree của start_unit_id.
            """
            leaves = set()
            stack = [start_unit_id]
            visited = set([start_unit_id])
            while stack:
                uid = stack.pop()
                childs = children_map.get(uid, [])
                if not childs:
                    leaves.add(uid)
                else:
                    for c in childs:
                        cid = c["id"]
                        if cid not in visited:
                            visited.add(cid)
                            stack.append(cid)
            return leaves

        all_manager_ids = set()
        all_leaf_unit_ids = set()

        for managed_id in managed_ids:
            # Lấy các đơn vị con trực tiếp của managed_id (nhánh cấp 1)
            direct_children = children_map.get(managed_id, [])
            if not direct_children:
                # Nếu managed_id không có con → coi chính nó là 1 nhánh để fallback lá
                direct_children = [{"id": managed_id}]

            # XỬ LÝ TỪNG NHÁNH CON
            for child in direct_children:
                cid = child["id"]

                mgrs = find_first_managers_bfs(cid, current_user_id=current_user_id)
                if mgrs:
                    # Có quản lý ở "độ sâu đầu tiên" của nhánh này → gom các quản lý này
                    all_manager_ids.update(mgrs)
                else:
                    # Không có quản lý ở bất kỳ cấp nào → gom tất cả lá của nhánh này
                    all_leaf_unit_ids.update(collect_leaf_units(cid))

        # 2) Query thông tin cho các quản lý đã tìm thấy
        result_rows = []
        if all_manager_ids:
            placeholders = ",".join(["%s"] * len(all_manager_ids))
            cursor.execute(
                f"SELECT * FROM `{DB_SCHEMA}`.employees2026 WHERE id IN ({placeholders})",
                tuple(all_manager_ids),
            )
            result_rows.extend(cursor.fetchall())

        # 3) Query nhân viên thuộc các phòng ban lá (loại bỏ current_user_id)
        if all_leaf_unit_ids:
            placeholders = ",".join(["%s"] * len(all_leaf_unit_ids))
            cursor.execute(
                f"""
                SELECT *
                FROM `{DB_SCHEMA}`.employees2026
                WHERE organization_unit_id IN ({placeholders})
                  AND id != %s
                """,
                tuple(all_leaf_unit_ids) + (current_user_id,),
            )
            result_rows.extend(cursor.fetchall())

        # 4) Khử trùng lặp + chuẩn hoá ngày
        seen = set()
        final = []
        for emp in result_rows:
            emp_id = emp.get("id")
            if emp_id and emp_id not in seen:
                seen.add(emp_id)
                for field in ("entry_date", "birth_date"):
                    if isinstance(emp.get(field), (date, datetime)):
                        emp[field] = emp[field].isoformat()
                final.append(emp)

        return jsonify(final)

    finally:
        cursor.close()
        conn.close()

@employees_bp.route('/mbo/score-final', methods=['PUT'])
def update_score_final():
    """
    Cập nhật trực tiếp score_final trong bảng `mbo_sessions`
    theo employee_id và mbo_year. Nếu không có bản ghi thì insert mới.

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
        # 1) Thử UPDATE trước
        cursor.execute(f"""
            UPDATE `{DB_SCHEMA}`.mbo_sessions
            SET score_final = %s
            WHERE employee_id = %s AND mbo_year = %s
        """, (score_final, employee_id, mbo_year))
        conn.commit()

        if cursor.rowcount > 0:
            # Đã cập nhật thành công
            return jsonify({
                "employee_id": employee_id,
                "mbo_year": mbo_year,
                "score_final": score_final,
                "created": False,
                "message": "score_final updated thành công"
            })

        # 2) Không có bản ghi -> INSERT mới
        #    Giả sử các cột khác (status, reviewer_id, ...) có default/NULL được.
        cursor.execute(f"""
            INSERT INTO `{DB_SCHEMA}`.mbo_sessions (employee_id, mbo_year, score_final)
            VALUES (%s, %s, %s)
        """, (employee_id, mbo_year, score_final))
        conn.commit()

        return jsonify({
            "employee_id": employee_id,
            "mbo_year": mbo_year,
            "score_final": score_final,
            "created": True,
            "message": "score_final inserted thành công"
        }), 201

    finally:
        cursor.close()
        conn.close()
@employees_bp.route('/by-code/<employee_code>', methods=['GET'])
def get_employee_summary_by_code(employee_code):
    from datetime import date
    mbo_year = request.args.get('mbo_year', type=int) or date.today().year

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")

    query = f"""
        WITH
        job AS (
            SELECT p.employee_code,
                   ROUND(SUM((COALESCE(p.approved_ey_score,0) * COALESCE(p.approver_ti_trong,0))/100), 2) AS job_score
            FROM `{DB_SCHEMA}`.personalmbo p
            WHERE p.mbo_year = %s
              AND p.employee_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
            GROUP BY p.employee_code
        ),
        comp AS (
            SELECT c.employee_code,
                   ROUND(SUM((COALESCE(c.approved_ey_score,0) * COALESCE(c.approver_ti_trong,0))/100), 2) AS competency_score
            FROM `{DB_SCHEMA}`.competencymbo c
            WHERE c.mbo_year = %s
              AND c.employee_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
            GROUP BY c.employee_code
        ),
        att AS (
            SELECT a.employee_code,
                   ROUND(AVG(a.score), 2) AS attitude_score
            FROM `{DB_SCHEMA}`.attitudembo a
            WHERE a.mbo_year = %s
              AND a.employee_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
            GROUP BY a.employee_code
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
            ms.score_final AS ms_score_final,
            COALESCE(ms.attitude_status, 'none') AS attitude_status,

            COALESCE(j.job_score, 0)         AS job_score,
            COALESCE(cp.competency_score, 0) AS competency_score,
            COALESCE(a.attitude_score, 0)    AS attitude_score,

            /* computed_final theo position */
            CASE
              /* Trưởng nhóm / Phó nhóm / Team lead / ' tl ' */
              WHEN  LOWER(e.position) REGEXP 'trưởng[[:space:]]*nhóm|truong[[:space:]]*nhom|phó[[:space:]]*nhóm|pho[[:space:]]*nhom|team[[:space:]]*lead'
                 OR  CONCAT(' ', LOWER(e.position), ' ') LIKE '% tl %'
              THEN ROUND( 0.10 * COALESCE(a.attitude_score,0)
                        + 0.45 * COALESCE(j.job_score,0)
                        + 0.45 * COALESCE(cp.competency_score,0), 2)

              /* Nhân viên */
              WHEN  LOWER(e.position) REGEXP 'nhân[[:space:]]*viên|nhan[[:space:]]*vien|staff|employee'
              THEN ROUND( 0.20 * COALESCE(a.attitude_score,0)
                        + 0.40 * COALESCE(j.job_score,0)
                        + 0.40 * COALESCE(cp.competency_score,0), 2)

              /* Cấp khác: bỏ thái độ */
              ELSE ROUND( 0.50 * COALESCE(j.job_score,0)
                        + 0.50 * COALESCE(cp.competency_score,0), 2)
            END AS computed_final,

            /* score_final ưu tiên ms.score_final */
            CASE
              WHEN ms.score_final IS NOT NULL THEN ROUND(ms.score_final, 2)
              ELSE
                CASE
                  WHEN  LOWER(e.position) REGEXP 'trưởng[[:space:]]*nhóm|truong[[:space:]]*nhom|phó[[:space:]]*nhóm|pho[[:space:]]*nhom|team[[:space:]]*lead'
                     OR  CONCAT(' ', LOWER(e.position), ' ') LIKE '% tl %'
                  THEN ROUND( 0.10 * COALESCE(a.attitude_score,0)
                            + 0.45 * COALESCE(j.job_score,0)
                            + 0.45 * COALESCE(cp.competency_score,0), 2)
                  WHEN  LOWER(e.position) REGEXP 'nhân[[:space:]]*viên|nhan[[:space:]]*vien|staff|employee'
                  THEN ROUND( 0.20 * COALESCE(a.attitude_score,0)
                            + 0.40 * COALESCE(j.job_score,0)
                            + 0.40 * COALESCE(cp.competency_score,0), 2)
                  ELSE ROUND( 0.50 * COALESCE(j.job_score,0)
                            + 0.50 * COALESCE(cp.competency_score,0), 2)
                END
            END AS score_final,

            /* Tên phòng ban nhỏ nhất */
            CASE
              WHEN e.group_name   IS NOT NULL AND e.group_name   <> '' THEN e.group_name
              WHEN e.section      IS NOT NULL AND e.section      <> '' THEN e.section
              WHEN e.sub_division IS NOT NULL AND e.sub_division <> '' THEN e.sub_division
              WHEN e.division     IS NOT NULL AND e.division     <> '' THEN e.division
              WHEN e.factory      IS NOT NULL AND e.factory      <> '' THEN e.factory
              WHEN e.company      IS NOT NULL AND e.company      <> '' THEN e.company
              WHEN e.corporation  IS NOT NULL AND e.corporation  <> '' THEN e.corporation
              ELSE NULL
            END AS department_name

        FROM `{DB_SCHEMA}`.employees2026 e
        LEFT JOIN job j    ON j.employee_code  COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
        LEFT JOIN comp cp  ON cp.employee_code COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
        LEFT JOIN att a    ON a.employee_code  COLLATE utf8mb4_unicode_ci = e.employee_code COLLATE utf8mb4_unicode_ci
        LEFT JOIN `{DB_SCHEMA}`.mbo_sessions ms
               ON ms.employee_id = e.id AND ms.mbo_year = %s
        WHERE e.employee_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
        LIMIT 1
    """

    try:
        # Param order: year/code, year/code, year/code, year, code
        params = (
            mbo_year, employee_code,
            mbo_year, employee_code,
            mbo_year, employee_code,
            mbo_year, employee_code
        )
        cursor.execute(query, params)
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "Not found", "employee_code": employee_code, "mbo_year": mbo_year}), 404
        return jsonify(row)
    finally:
        cursor.close()
        conn.close()
