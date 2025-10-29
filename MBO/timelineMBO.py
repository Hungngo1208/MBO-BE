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

# -------------------------
# DB ENSURE / HELPERS
# -------------------------

def ensure_table():
    """
    Tạo bảng nếu chưa có:
    - mbo_timelines
    - mbo_years (lưu trạng thái theo từng năm)
    Đồng thời migrate bỏ cột enabled nếu còn trong mbo_timelines.
    """
    db = get_connection()
    cur = db.cursor()
    # timelines
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

    # years (status theo từng năm)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mbo_years (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          year INT NOT NULL,
          status ENUM('active','inactive') NOT NULL DEFAULT 'inactive',
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_mbo_years_year (year)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    db.commit()
    cur.close()
    db.close()

def _ensure_year_has_5_rows(db, year: int):
    """
    Đảm bảo đủ 5 phase cho 1 năm. Khi chèn mới -> status='inactive' để nhất quán.
    """
    cur = db.cursor()
    for ph in VALID_PHASES.keys():
        cur.execute(
            "SELECT 1 FROM mbo_timelines WHERE mbo_year=%s AND phase=%s LIMIT 1",
            (year, ph),
        )
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO mbo_timelines (mbo_year, phase, status) VALUES (%s,%s,'inactive')",
                (year, ph),
            )
    db.commit()
    cur.close()

def ensure_year_row(cur, db, year: int, default_status="inactive"):
    # tạo row nếu chưa có
    cur.execute("SELECT COUNT(*) FROM mbo_timeline WHERE year = %s", (year,))
    (cnt,) = cur.fetchone()
    if cnt == 0:
        cur.execute(
            "INSERT INTO mbo_timeline (year, status) VALUES (%s, %s)",
            (year, default_status),
        )

def set_year_status(cur, db, year: int, status: str):
    # KHÔNG set updated_at nữa
    cur.execute(
        "UPDATE mbo_timeline SET status=%s WHERE year=%s",
        (status, year),
    )


def get_year_status(cur, db, year: int) -> str:
    """
    Lấy status của một năm; nếu chưa có row thì tạo mặc định inactive rồi trả về.
    """
    ensure_year_row(cur, db, year, default_status="inactive")
    cur.execute("SELECT status FROM mbo_years WHERE year=%s", (year,))
    r = cur.fetchone()
    return r[0] if r else "inactive"

# --------------------------------
# 1) GET timeline by year (KHÔNG ĐỔI)
# --------------------------------
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

# --------------------------------
# 2) PUT upsert timeline for a year (KHÔNG ĐỔI)
# --------------------------------
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

        st_norm = (st or "inactive")
        normalized.append((phase, s, e, st_norm))

    db = get_connection()
    cur = db.cursor()
    try:
        _ensure_year_has_5_rows(db, mbo_year)

        sql_sel = "SELECT id FROM mbo_timelines WHERE mbo_year=%s AND phase=%s"
        sql_ins = """
            INSERT INTO mbo_timelines (mbo_year, phase, start_date, end_date, status)
            VALUES (%s,%s,%s,%s,%s)
        """
        sql_upd = """
            UPDATE mbo_timelines
               SET start_date=%s, end_date=%s, status=%s, updated_at=CURRENT_TIMESTAMP
             WHERE mbo_year=%s AND phase=%s
        """

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

# --------------------------------
# 3) POST reset timeline of a year
#    (giữ reset timeline; đồng thời set status năm đó về 'inactive' trong mbo_years)
# --------------------------------
@mbo_timeline_bpp.route("/mbo/timeline/<int:mbo_year>/reset", methods=["POST"])
def reset_year(mbo_year: int):
    db = get_connection()
    cur = db.cursor()
    try:
        _ensure_year_has_5_rows(db, mbo_year)
        # Reset ngày và status về 'inactive' để nhất quán với FE
        cur.execute(
            """
            UPDATE mbo_timelines
               SET start_date=NULL,
                   end_date=NULL,
                   status='inactive',
                   updated_at=CURRENT_TIMESTAMP
             WHERE mbo_year=%s
            """,
            (mbo_year,),
        )

        # Đảm bảo có row năm và set status năm đó về inactive trong mbo_years
        ensure_year_row(cur, db, mbo_year, default_status="inactive")
        set_year_status(cur, db, mbo_year, "inactive")

        db.commit()
        return jsonify({"ok": True})
    finally:
        cur.close()
        db.close()

# --------------------------------
# --- SETTINGS (chỉ có 1 dòng) ---
# --------------------------------

# GET /mbo/settings — giữ nguyên tên.
#   - Trả current_year như cũ.
#   - Nhận thêm query ?year=YYYY để trả status của năm đó từ mbo_years.
#   - Nếu không truyền year => dùng current_year.
@mbo_timeline_bpp.route("/mbo/settings", methods=["GET"])
def get_settings():
    db = get_connection()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("SELECT current_year FROM mbo_settings WHERE id = 1")
        row = cur.fetchone()

        if not row:
            year_now = datetime.now().year
            cur2 = db.cursor()
            try:
                cur2.execute(
                    """
                    INSERT INTO mbo_settings (id, current_year)
                    VALUES (1, %s)
                    ON DUPLICATE KEY UPDATE current_year = VALUES(current_year)
                    """,
                    (year_now,),
                )
                db.commit()
                current_year = year_now
            finally:
                cur2.close()
        else:
            current_year = int(row["current_year"])

        # year cần lấy status: ưu tiên query param ?year=, mặc định = current_year
        year_param = request.args.get("year", type=int) or current_year

        # Lấy status từ mbo_years (nếu chưa có thì tạo inactive)
        cur3 = db.cursor()
        try:
            status = get_year_status(cur3, db, year_param)
        finally:
            cur3.close()

        return jsonify({
            "current_year": current_year,
            "year": year_param,
            "status": status
        })
    finally:
        cur.close()
        db.close()

# PUT /mbo/settings — giữ nguyên tên. Chỉ cập nhật current_year, không lưu status.
@mbo_timeline_bpp.route("/mbo/settings", methods=["PUT"])
def update_settings():
    payload = request.get_json(silent=True) or {}
    year = payload.get("current_year")
    if not isinstance(year, int):
        return jsonify({"error": "current_year phải là số nguyên"}), 400

    db = get_connection()
    cur = db.cursor()
    try:
        # đảm bảo có bản ghi id=1
        cur.execute("SELECT id FROM mbo_settings WHERE id=1")
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE mbo_settings SET current_year=%s WHERE id=1", (year,))
        else:
            cur.execute("INSERT INTO mbo_settings (id, current_year) VALUES (1, %s)", (year,))

        # đảm bảo năm tồn tại trong mbo_years với mặc định inactive nếu chưa có
        ensure_year_row(cur, db, year, default_status="inactive")

        db.commit()
        return jsonify({"ok": True, "current_year": year})
    finally:
        cur.close()
        db.close()

# --------------------------------
# 4) PUT /mbo/settings/status — GIỮ NGUYÊN TÊN
#     Body: {"year": 2025, "status": "active" | "inactive"}
# --------------------------------
@mbo_timeline_bpp.route("/mbo/settings/status", methods=["PUT"])
def update_status_active():
    """
    Cập nhật trạng thái theo năm trong bảng nsh.mbo_years:
    - year: số nguyên (bắt buộc)
    - status: 'active' hoặc 'inactive'
    """
    payload = request.get_json(silent=True) or {}
    try:
        year = int(payload.get("year"))
    except Exception:
        return jsonify({"error": "year phải là số nguyên"}), 400

    status = (payload.get("status") or "").strip().lower()
    if status not in ("active", "inactive"):
        return jsonify({"error": "status phải là 'active' hoặc 'inactive'"}), 400

    db = get_connection()
    cur = db.cursor()
    try:
        # 1️⃣ Kiểm tra có dòng năm này chưa
        cur.execute("SELECT id FROM nsh.mbo_years WHERE year = %s", (year,))
        row = cur.fetchone()

        # 2️⃣ Nếu chưa có thì tạo mới
        if row is None:
            cur.execute(
                "INSERT INTO nsh.mbo_years (year, status) VALUES (%s, %s)",
                (year, status)
            )
        else:
            # 3️⃣ Nếu đã có thì cập nhật trạng thái
            cur.execute(
                "UPDATE nsh.mbo_years SET status = %s WHERE year = %s",
                (status, year)
            )

        db.commit()
        return jsonify({
            "message": f"Đã cập nhật status của năm {year} thành '{status}'",
            "year": year,
            "status": status
        }), 200

    except Exception as e:
        db.rollback()
        return jsonify({"error": f"Lỗi cập nhật trạng thái: {str(e)}"}), 500
    finally:
        try: cur.close()
        except: pass
        try: db.close()
        except: pass