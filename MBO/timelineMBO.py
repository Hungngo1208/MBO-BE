from flask import Blueprint, request, jsonify
from database import get_connection
from datetime import datetime

mbo_timeline_bpp = Blueprint("mbo_timeline_bpp", __name__)

VALID_PHASES = {
    "create": "Lập MBO",
    "early_review": "Đánh giá đầu năm",
    "self_assessment": "Tự đánh giá cuối năm",
    "final_review": "Đánh giá cuối năm",
    "official_result": "Kết quả chính thức",
}

def _is_valid_date(s: str) -> bool:
    if s is None:
        return True
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False

def _is_valid_status(s):
    if s is None:
        return True
    return str(s).lower() in ("active", "inactive")

def ensure_table():
    """Tạo bảng nếu chưa có + migrate nhẹ để chỉ còn status"""
    db = get_connection()
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mbo_timelines (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          mbo_year INT NOT NULL,
          phase ENUM('create','early_review','self_assessment','final_review','official_result') NOT NULL,
          start_date DATE NULL,
          end_date DATE NULL,
          status ENUM('active','inactive') NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_year_phase (mbo_year, phase)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # migrate bỏ enabled nếu còn
    try:
        cur.execute("ALTER TABLE mbo_timelines DROP COLUMN enabled")
    except Exception:
        pass
    db.commit()
    cur.close()
    db.close()

def _ensure_year_has_5_rows(db, year: int):
    cur = db.cursor()
    for ph in VALID_PHASES.keys():
        cur.execute("SELECT 1 FROM mbo_timelines WHERE mbo_year=%s AND phase=%s LIMIT 1", (year, ph))
        if not cur.fetchone():
            cur.execute("INSERT INTO mbo_timelines (mbo_year, phase) VALUES (%s,%s)", (year, ph))
    db.commit()
    cur.close()

# 1) GET
@mbo_timeline_bpp.route("/mbo/timeline/<int:mbo_year>", methods=["GET"])
def get_timeline_by_year(mbo_year: int):
    db = get_connection()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, mbo_year, phase, start_date, end_date, status
            FROM mbo_timelines
            WHERE mbo_year = %s
            ORDER BY FIELD(phase,'create','early_review','self_assessment','final_review','official_result')
            """,
            (mbo_year,),
        )
        rows = cur.fetchall()
        return jsonify({"year": mbo_year, "items": rows})
    finally:
        cur.close()
        db.close()

# 2) PUT
@mbo_timeline_bpp.route("/mbo/timeline/<int:mbo_year>", methods=["PUT"])
def upsert_timeline_for_year(mbo_year: int):
    payload = request.get_json(silent=True) or {}
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return jsonify({"error": "items rỗng"}), 400

    normalized = []
    for it in items:
        phase = (it.get("phase") or "").strip()
        if phase not in VALID_PHASES:
            return jsonify({"error": f"phase không hợp lệ: {phase}"}), 400
        s = it.get("start_date")
        e = it.get("end_date")
        st = it.get("status", None)
        if not _is_valid_date(s) or not _is_valid_date(e):
            return jsonify({"error": f"Ngày không hợp lệ ở {phase}"}), 400
        if s and e and s > e:
            return jsonify({"error": f"start_date > end_date ở {phase}"}), 400
        if not _is_valid_status(st):
            return jsonify({"error": f"status không hợp lệ ở {phase}"}), 400
        normalized.append((phase, s, e, st))

    db = get_connection()
    cur = db.cursor()
    try:
        _ensure_year_has_5_rows(db, mbo_year)
        sql_sel = "SELECT id FROM mbo_timelines WHERE mbo_year=%s AND phase=%s"
        sql_ins = "INSERT INTO mbo_timelines (mbo_year, phase, start_date, end_date, status) VALUES (%s,%s,%s,%s,%s)"
        sql_upd = "UPDATE mbo_timelines SET start_date=%s,end_date=%s,status=%s,updated_at=CURRENT_TIMESTAMP WHERE mbo_year=%s AND phase=%s"

        for phase, s, e, st in normalized:
            cur.execute(sql_sel, (mbo_year, phase))
            if not cur.fetchone():
                cur.execute(sql_ins, (mbo_year, phase, s, e, st))
            else:
                cur.execute(sql_upd, (s, e, st, mbo_year, phase))

        db.commit()
        return jsonify({"ok": True})
    finally:
        cur.close()
        db.close()

# 3) RESET
@mbo_timeline_bpp.route("/mbo/timeline/<int:mbo_year>/reset", methods=["POST"])
def reset_year(mbo_year: int):
    db = get_connection()
    cur = db.cursor()
    try:
        _ensure_year_has_5_rows(db, mbo_year)
        cur.execute(
            "UPDATE mbo_timelines SET start_date=NULL,end_date=NULL,status=NULL,updated_at=CURRENT_TIMESTAMP WHERE mbo_year=%s",
            (mbo_year,),
        )
        db.commit()
        return jsonify({"ok": True})
    finally:
        cur.close()
        db.close()
# --- SETTINGS (chỉ có 1 dòng) ---

@mbo_timeline_bpp.route("/mbo/settings", methods=["GET"])
def get_settings():
    db = get_connection()
    cur = db.cursor(dictionary=True)
    try:
        # luôn lấy đúng 1 bản ghi theo id = 1
        cur.execute("SELECT current_year FROM mbo_settings WHERE id = 1")
        row = cur.fetchone()

        if not row:
            year_now = datetime.now().year
            cur2 = db.cursor()
            try:
                # tạo bản ghi id=1 nếu chưa có; nếu có thì cập nhật
                cur2.execute("""
                    INSERT INTO mbo_settings (id, current_year)
                    VALUES (1, %s)
                    ON DUPLICATE KEY UPDATE current_year = VALUES(current_year)
                """, (year_now,))
                db.commit()
                row = {"current_year": year_now}
            finally:
                cur2.close()

        # đảm bảo format JSON nhất quán
        return jsonify({"current_year": int(row["current_year"])})
    finally:
        cur.close()
        db.close()
@mbo_timeline_bpp.route("/mbo/settings", methods=["PUT"])
def update_settings():
    payload = request.get_json(silent=True) or {}
    year = payload.get("current_year")
    if not isinstance(year, int):
        return jsonify({"error": "current_year phải là số nguyên"}), 400

    db = get_connection()
    cur = db.cursor()
    try:
        # luôn đảm bảo bảng chỉ có 1 row
        cur.execute("SELECT id FROM mbo_settings LIMIT 1")
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE mbo_settings SET current_year=%s WHERE id=%s", (year, row[0]))
        else:
            cur.execute("INSERT INTO mbo_settings (current_year) VALUES (%s)", (year,))
        db.commit()
        return jsonify({"ok": True, "current_year": year})
    finally:
        cur.close()
        db.close()
