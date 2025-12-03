# attitude.py
from flask import Blueprint, request, jsonify
from database import get_connection
from decimal import Decimal
from typing import List, Dict, Optional  # <<< thêm dòng này

attitude_bp = Blueprint("attitude", __name__)

# ==== Helpers ====
CUR_ITEMS = (
    "Ý thức trách nhiệm",
    "Thái độ tích cực",
    "Thái độ hợp tác",
    "Chấp hành kỷ luật",
)

def _require_params(params: Dict, required_keys: List[str]) -> Optional[str]:
    missing = [k for k in required_keys if params.get(k) in (None, "")]
    if missing:
        return f"Thiếu tham số: {', '.join(missing)}"
    return None

def _normalize_title(s: str) -> str:
    return " ".join((s or "").strip().split())

def _jsonify_row(row: Dict) -> Dict:
    if not row:
        return row
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (bytes, bytearray)):
            out[k] = v.decode("utf-8", errors="ignore")
        else:
            out[k] = v
    return out

def _jsonify_rows(rows: List[Dict]) -> List[Dict]:
    return [_jsonify_row(r) for r in rows]

def _get_employee_id(cur, employee_code: str):
    cur.execute("SELECT id FROM employees2026 WHERE employee_code=%s LIMIT 1", (employee_code,))
    row = cur.fetchone()
    return row["id"] if row and row.get("id") else None

def _count_scored_items(cur, employee_code: str, mbo_year: int) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
          FROM attitudembo
         WHERE employee_code=%s AND mbo_year=%s
           AND goal_title IN (%s, %s, %s, %s)
        """,
        (employee_code, mbo_year, *CUR_ITEMS),
    )
    row = cur.fetchone() or {"cnt": 0}
    return int(row["cnt"] or 0)

def _set_attitude_status(conn, employee_id: int, mbo_year: int, is_scored: bool):
    cur = conn.cursor()
    # chỉ update nếu session có tồn tại, không tự insert
    cur.execute(
        "UPDATE mbo_sessions SET attitude_status=%s WHERE employee_id=%s AND mbo_year=%s",
        ("scored" if is_scored else None, employee_id, mbo_year),
    )
    conn.commit()

# ===== API 1: Lấy danh sách điểm của nhân viên =====
@attitude_bp.route("/list", methods=["GET"])
def list_scores_by_employee_year():
    """
    GET /attitude/list?employee_code=E001&mbo_year=2025
    """
    employee_code = request.args.get("employee_code", type=str)
    mbo_year = request.args.get("mbo_year", type=int)

    err = _require_params(
        {"employee_code": employee_code, "mbo_year": mbo_year},
        ["employee_code", "mbo_year"],
    )
    if err:
        return jsonify({"error": err}), 400

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, employee_code, mbo_year, goal_title, score
            FROM attitudembo
            WHERE employee_code=%s AND mbo_year=%s
            ORDER BY id ASC
            """,
            (employee_code, mbo_year),
        )
        rows = cur.fetchall()
        return jsonify({"data": _jsonify_rows(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# ===== API 2: Cập nhật nhiều mục trong 1 lần =====
@attitude_bp.route("/scores", methods=["PUT"])
def upsert_scores_bulk():
    """
    PUT /attitude/scores
    Body:
    {
      "employee_code": "E001",
      "mbo_year": 2025,
      "items": [
        {"goal_title": "Ý thức trách nhiệm", "score": 80},
        {"goal_title": "Thái độ tích cực", "score": 75},
        ...
      ]
    }

    - Nếu là LẦN ĐẦU (chưa có bản ghi nào cho employee_code + year):
        * BẮT BUỘC phải gửi đủ 4 mục (đúng 4 title cố định) và score hợp lệ → mới cho lưu.
        * Sau khi lưu đủ 4, set attitude_status = 'scored'.
    - Nếu ĐÃ CÓ dữ liệu:
        * Cho phép gửi 1..4 mục, mục nào có thì UPDATE/INSERT mục đó.
        * Sau khi cập nhật, nếu đủ 4 mục → 'scored', nếu <4 → NULL.
    """
    body = request.get_json(silent=True) or {}
    employee_code = body.get("employee_code")
    mbo_year = body.get("mbo_year")
    items = body.get("items") or []

    err = _require_params(
        {"employee_code": employee_code, "mbo_year": mbo_year},
        ["employee_code", "mbo_year"],
    )
    if err:
        return jsonify({"error": err}), 400

    try:
        mbo_year = int(mbo_year)
    except Exception:
        return jsonify({"error": "mbo_year phải là số nguyên"}), 400

    # Chuẩn hoá + validate items
    norm_items = []
    seen_titles = set()
    for it in items:
        title = _normalize_title(it.get("goal_title"))
        if not title:
            return jsonify({"error": "Mỗi item cần goal_title"}), 400
        if title not in CUR_ITEMS:
            return jsonify({"error": f"goal_title không hợp lệ: '{title}'"}), 400
        if title in seen_titles:
            return jsonify({"error": f"Trùng goal_title trong payload: '{title}'"}), 400
        seen_titles.add(title)
        try:
            val = float(it.get("score"))
        except Exception:
            return jsonify({"error": f"score không hợp lệ cho mục '{title}'"}), 400
        norm_items.append((title, val))

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # Xác định lần đầu hay không
        existing_cnt = _count_scored_items(cur, employee_code, mbo_year)
        is_first_time = (existing_cnt == 0)

        if is_first_time:
            # LẦN ĐẦU → bắt buộc đủ 4 mục, và phải đúng 4 title cố định
            if len(norm_items) != 4 or set(t for t, _ in norm_items) != set(CUR_ITEMS):
                return jsonify({
                    "error": "Lần đầu lưu phải gửi đủ 4 mục: Ý thức trách nhiệm, Thái độ tích cực, Thái độ hợp tác, Chấp hành kỷ luật."
                }), 400

        # Cập nhật từng mục: UPDATE trước, nếu 0 dòng thì INSERT
        updated, inserted = 0, 0
        for title, val in norm_items:
            cur.execute(
                """
                UPDATE attitudembo
                   SET score=%s
                 WHERE employee_code=%s AND mbo_year=%s AND goal_title=%s
                """,
                (val, employee_code, mbo_year, title),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO attitudembo (employee_code, mbo_year, goal_title, score)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (employee_code, mbo_year, title, val),
                )
                inserted += 1
            else:
                updated += 1

        conn.commit()

        # Recount để quyết định trạng thái
        total_now = _count_scored_items(cur, employee_code, mbo_year)
        is_scored = (total_now >= 4)

        # Map employee_code -> employee_id và update attitude_status
        employee_id = _get_employee_id(cur, employee_code)
        if employee_id:
            _set_attitude_status(conn, employee_id, mbo_year, is_scored)

        # Trả lại toàn bộ danh sách hiện tại
        cur.execute(
            """
            SELECT id, employee_code, mbo_year, goal_title, score
              FROM attitudembo
             WHERE employee_code=%s AND mbo_year=%s
             ORDER BY id ASC
            """,
            (employee_code, mbo_year),
        )
        rows = cur.fetchall()

        return jsonify({
            "message": "OK",
            "updated": updated,
            "inserted": inserted,
            "attitude_status": "scored" if is_scored else None,
            "data": _jsonify_rows(rows),
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()
