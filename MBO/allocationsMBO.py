from flask import Blueprint, request, jsonify
from database import get_connection

allocations_bp = Blueprint("allocations", __name__)

def _require_mbo_year(payload):
    # Lấy năm từ body (bắt buộc)
    year = payload.get("mbo_year")
    try:
        year = int(year)
    except (TypeError, ValueError):
        return None
    if year < 2000 or year > 2100:
        return None
    return year

@allocations_bp.route("/allocations", methods=["POST"])
def create_allocations():
    """
    Body: [
      {
        "goal_id": 123,
        "sender_code": "E001",
        "receiver_code": "E002",
        "allocation_value": 30,
        "mbo_year": 2025
      },
      ...
    ]
    """
    data = request.get_json()

    if not data or not isinstance(data, list):
        return jsonify({"error": "Dữ liệu không hợp lệ. Cần truyền vào một mảng các phân bổ."}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            INSERT INTO mbo_allocations
            (goal_id, sender_code, receiver_code, mbo_year, allocation_value, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """

        inserted = 0
        skipped = []

        for idx, item in enumerate(data):
            goal_id = item.get("goal_id")
            sender_code = item.get("sender_code")
            receiver_code = item.get("receiver_code")
            allocation_value = item.get("allocation_value")
            mbo_year = _require_mbo_year(item)

            # Bỏ qua bản ghi thiếu thông tin bắt buộc
            if not all([goal_id, sender_code, receiver_code]) or allocation_value is None or mbo_year is None:
                skipped.append(idx)
                continue

            cursor.execute(query, (goal_id, sender_code, receiver_code, mbo_year, allocation_value))
            inserted += 1

        conn.commit()
        return jsonify({
            "message": "Thêm danh sách phân bổ thành công.",
            "inserted": inserted,
            "skipped_indexes": skipped
        }), 201

    except Exception as e:
        print("Lỗi:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


@allocations_bp.route("/allocations/by-sender", methods=["POST"])
def get_allocations_by_sender():
    """
    Body:
    {
      "sender_code": "E001",
      "mbo_year": 2025,
      "goal_id": 123  # optional
    }
    """
    data = request.get_json() or {}
    sender_code = data.get("sender_code")
    if not sender_code:
        return jsonify({"error": "Thiếu sender_code"}), 400

    mbo_year = _require_mbo_year(data)
    if mbo_year is None:
        return jsonify({"error": "Thiếu hoặc sai định dạng mbo_year (2000..2100)"}), 400

    goal_id = data.get("goal_id")  # Optional

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Lưu ý: bảng nhân viên ở hệ của bạn có thể là employees2026, nếu vậy đổi JOIN cho đúng:
        # JOIN employees2026 e ON a.receiver_code = e.employee_code
        query = """
            SELECT a.id, a.goal_id, a.sender_code, a.receiver_code, a.allocation_value, a.created_at,
                   e.full_name AS receiver_fullname
            FROM mbo_allocations a
            JOIN employees e ON a.receiver_code = e.code
            WHERE a.sender_code = %s
              AND a.mbo_year = %s
        """
        params = [sender_code, mbo_year]

        if goal_id:
            query += " AND a.goal_id = %s"
            params.append(goal_id)

        query += " ORDER BY a.id DESC"

        cursor.execute(query, params)
        results = cursor.fetchall()

        return jsonify({
            "items": results,
            "mbo_year": mbo_year
        }), 200

    except Exception as e:
        print("Lỗi:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass
