from flask import Blueprint, jsonify, request
from database import get_connection

department_bp = Blueprint('department', __name__, url_prefix='/department')

# ====================================
# GET /tree - Trả về cây tổ chức
# ====================================
@department_bp.route('/tree', methods=['GET'])
def get_department_tree():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT id, name, type, parent_id, code, employee_count, created_at, updated_at, employee_id
        FROM organization_units
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Build cây từ parent_id
    node_map = {row["id"]: {**row, "_children": []} for row in rows}
    roots = []

    for node in node_map.values():
        parent_id = node["parent_id"]
        if parent_id and parent_id in node_map:
            node_map[parent_id]["_children"].append(node)
        else:
            roots.append(node)

    def build_tree(nodes, prefix=""):
        result = []
        import re

        def extract_code_number(code):
            if not code:
                return float('inf')  # Đẩy những mã trống xuống cuối
            match = re.search(r'(\d+)$', code)
            return int(match.group(1)) if match else float('inf')

        for idx, node in enumerate(sorted(nodes, key=lambda x: extract_code_number(x["code"]))):
            children = build_tree(node["_children"], f"{prefix}{idx}-")
            result.append({
                "key": f"{prefix}{idx}",
                "data": {
                    "id": node["id"],
                    "name": node["name"],
                    "type": node["type"],
                    "code": node["code"],
                    "parent_id": node["parent_id"],
                    "employee_count": node["employee_count"] or 0,
                    "employee_id": node["employee_id"],  # ✅ Thêm dòng này
                },
                "children": children if children else None
            })
        return result

    return jsonify(build_tree(roots))



# ====================================
# POST /add - Thêm bộ phận
# ====================================
@department_bp.route('/add', methods=['POST'])
def add_department():
    data = request.json
    name = data.get("name")
    type = data.get("type")
    parent_id = data.get("parent_id")
    code = data.get("code", None)
    employee_id = data.get("employee_id")  # <- thêm dòng này

    if not name or not type:
        return jsonify({"error": "Thiếu trường name hoặc type"}), 400

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO organization_units (name, type, parent_id, code, employee_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (name.strip(), type.strip(), parent_id, code, employee_id))
        conn.commit()
        return jsonify({"message": "Đã thêm bộ phận", "id": cursor.lastrowid}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()
@department_bp.route('/update', methods=['PATCH'])
def update_department():
    data = request.json
    unit_id = data.get("id")

    if not unit_id:
        return jsonify({"error": "Thiếu ID"}), 400

    allowed_fields = ["name", "type", "parent_id", "code", "employee_id"]
    updates = []
    values = []

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # Lấy dữ liệu cũ
        cursor.execute("SELECT * FROM organization_units WHERE id = %s", (unit_id,))
        current = cursor.fetchone()

        if not current:
            return jsonify({"error": "Không tìm thấy bộ phận"}), 404

        old_name = current["name"]
        old_type = current["type"]

        print("Dữ liệu gửi lên:", data)

        # Lọc và build câu lệnh UPDATE
        for field in allowed_fields:
            if field in data and data[field] not in [None, ""]:
                updates.append(f"`{field}` = %s")
                value = data[field].strip() if isinstance(data[field], str) else data[field]
                values.append(value)

        if not updates:
            return jsonify({"error": "Không có trường nào để cập nhật"}), 400

        values.append(unit_id)

        update_sql = f"""
            UPDATE organization_units
            SET {', '.join(updates)}
            WHERE id = %s
        """

        print("Câu lệnh UPDATE:", update_sql)
        print("Giá trị:", values)

        cursor2 = conn.cursor()
        cursor2.execute(update_sql, values)

        # Nếu có đổi tên thì cập nhật bảng employees2026 tương ứng
        if "name" in data and data["name"].strip():
            new_name = data["name"].strip()

            type_to_column = {
                "corporation": "corporation",
                "company": "company",
                "factory": "factory",
                "division": "division",
                "sub_division": "sub_division",
                "section": "section",
                "group": "group_name"
            }

            column_name = type_to_column.get(old_type)

            if column_name:
                update_emps = conn.cursor()
                update_emps.execute(
                    f"""UPDATE employees2026
                        SET `{column_name}` = %s
                        WHERE `{column_name}` = %s
                    """,
                    (new_name, old_name)
                )
                update_emps.close()

        conn.commit()
        return jsonify({"message": "Cập nhật thành công"})

    except Exception as e:
        conn.rollback()
        import traceback
        print("Lỗi cập nhật:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()

# ====================================
# DELETE /delete - Xoá bộ phận
# ====================================
@department_bp.route('/delete', methods=['DELETE'])
def delete_department():
    data = request.json
    unit_id = data.get("id")

    if not unit_id:
        return jsonify({"error": "Thiếu ID"}), 400

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM organization_units WHERE id = %s", (unit_id,))
        conn.commit()
        return jsonify({"message": "Đã xoá bộ phận"})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()
