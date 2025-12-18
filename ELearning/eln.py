# eln.py
import os
import uuid
from flask import Blueprint, request, jsonify, send_from_directory
from flask import current_app as app
from werkzeug.utils import secure_filename
from database import get_connection

eln_bp = Blueprint("eln", __name__)

# ============================================================
# CẤU HÌNH CỨNG: LUÔN LƯU MEDIA Ở FILE SERVER NÀY
# ============================================================
MEDIA_ROOT = r"\\10.73.131.2\eln_media"

# (Tùy chọn) Nếu muốn cho phép override bằng env, đổi thành:
# MEDIA_ROOT = os.getenv("ELN_UPLOAD_DIR") or r"\\10.73.131.2\media"

BASE_UPLOAD = MEDIA_ROOT
VIDEO_DIR = os.path.join(BASE_UPLOAD, "videos")
COVER_DIR = os.path.join(BASE_UPLOAD, "covers")

# Tạo thư mục trên UNC share (đòi hỏi service account có quyền ghi)
os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(COVER_DIR, exist_ok=True)

ALLOWED_IMAGE = {"png", "jpg", "jpeg", "gif", "webp"}
ALLOWED_VIDEO = {"mp4", "mov", "avi", "mkv", "webm"}

# ========== Giá trị mặc định cho mapping ==========
DEFAULT_STATUS = "fail"
DEFAULT_TRAINING_TYPE = "Đào tạo lần đầu"
DEFAULT_HIEN_TRANG = "Chưa đào tạo"
DEFAULT_GAN_NHAT = "Chưa đào tạo"
DEFAULT_STATUS_WATCH = "fail"


def _ext_ok(filename, allow_set):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in allow_set


def _save_file(file_storage, folder, allow_set):
    """
    Lưu file vào UNC share (\\10.73.131.2\media\videos or covers) với tên UUID.
    DB chỉ lưu path tương đối theo BASE_UPLOAD:
      - videos/<uuid>.mp4
      - covers/<uuid>.jpg
    """
    if not file_storage or file_storage.filename == "":
        return None

    fname = secure_filename(file_storage.filename)
    if not _ext_ok(fname, allow_set):
        return None

    ext = fname.rsplit(".", 1)[1].lower()
    new_name = f"{uuid.uuid4().hex}.{ext}"

    abs_path = os.path.join(folder, new_name)
    file_storage.save(abs_path)

    rel_path = os.path.relpath(abs_path, BASE_UPLOAD).replace("\\", "/")
    return rel_path


def _abs_from_rel(rel_path: str):
    """
    Convert DB path -> absolute path trên UNC share.
    Hỗ trợ:
      - videos/xxx.mp4
      - covers/yyy.jpg
    Nếu DB lỡ lưu absolute path thì trả nguyên.
    """
    if not rel_path:
        return None

    rp = rel_path.replace("\\", "/")

    # absolute path (C:\..., /..., UNC: //10.73...)
    if os.path.isabs(rp) or rp.startswith("//"):
        return rp

    return os.path.join(BASE_UPLOAD, rp.replace("/", os.sep))


def _safe_remove_file(rel_path: str) -> bool:
    """
    Xoá file theo đường dẫn lưu trong DB. Không raise exception.
    Xoá trên UNC share.
    """
    if not rel_path:
        return False
    try:
        abs_path = _abs_from_rel(rel_path)
        if abs_path and os.path.isfile(abs_path):
            os.remove(abs_path)
            app.logger.info(f"[ELN] Removed file: {abs_path}")
            return True
        return False
    except Exception as e:
        app.logger.warning(f"[ELN] Failed to remove file ({rel_path}): {e}")
        return False


# ========== FILE SERVE ==========
# IMPORTANT:
# - DB lưu "videos/<file>" nên client phải gọi:
#   /files/eln/videos/<file>
# - DB lưu "covers/<file>" nên client gọi:
#   /files/eln/covers/<file>
@eln_bp.route("/files/eln/videos/<path:filename>")
def serve_video(filename):
    # filename nên là "<uuid>.mp4"
    return send_from_directory(VIDEO_DIR, filename, as_attachment=False)


@eln_bp.route("/files/eln/covers/<path:filename>")
def serve_cover(filename):
    # filename nên là "<uuid>.jpg"
    return send_from_directory(COVER_DIR, filename, as_attachment=False)


# ========== APIs ==========
@eln_bp.route("/eln", methods=["GET"])
def list_eln():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          id,
          title,
          CAST(positions AS CHAR) AS positions,
          training_time,
          note,
          video_path,
          cover_path,
          COALESCE(tong_nhan_vien_hoc, 0) AS tong_nhan_vien_hoc,
          COALESCE(so_nhan_vien_hoan_thanh, 0) AS so_nhan_vien_hoan_thanh,
          DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%s') AS created_at,
          DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%s') AS updated_at
        FROM eln
        ORDER BY id DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows), 200


@eln_bp.route("/eln/<int:item_id>", methods=["GET"])
def get_eln(item_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          id,
          title,
          CAST(positions AS CHAR) AS positions,
          training_time,
          note,
          video_path,
          cover_path,
          COALESCE(tong_nhan_vien_hoc, 0) AS tong_nhan_vien_hoc,
          COALESCE(so_nhan_vien_hoan_thanh, 0) AS so_nhan_vien_hoan_thanh,
          DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%s') AS created_at,
          DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%s') AS updated_at
        FROM eln
        WHERE id = %s
    """, (item_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify(row), 200


@eln_bp.route("/eln/<int:item_id>", methods=["PUT"])
def update_eln(item_id):
    title = request.form.get("title", "").strip()
    positions = request.form.get("positions")
    training_time = request.form.get("training_time") or None
    note = request.form.get("note")

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM eln WHERE id=%s", (item_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Not found"}), 404

    old_video_path = row.get("video_path")
    old_cover_path = row.get("cover_path")

    # mặc định giữ cũ
    video_path = old_video_path
    cover_path = old_cover_path

    uploaded_new_video = False
    uploaded_new_cover = False

    # Nhận & lưu video mới (UNC)
    if "video" in request.files and request.files["video"].filename:
        new_video = _save_file(request.files["video"], VIDEO_DIR, ALLOWED_VIDEO)
        if not new_video:
            cur.close(); conn.close()
            return jsonify({"error": "Invalid video format"}), 400
        video_path = new_video
        uploaded_new_video = True

    # Nhận & lưu cover mới (UNC)
    if "cover" in request.files and request.files["cover"].filename:
        new_cover = _save_file(request.files["cover"], COVER_DIR, ALLOWED_IMAGE)
        if not new_cover:
            cur.close(); conn.close()
            return jsonify({"error": "Invalid image format"}), 400
        cover_path = new_cover
        uploaded_new_cover = True

    new_title = title or row["title"]
    new_positions_str = positions if positions is not None else row["positions"]
    new_training_time = training_time if training_time is not None else row["training_time"]
    new_note = note if note is not None else row["note"]

    # Nếu client gửi positions nhưng rỗng -> từ chối (và rollback file mới)
    if positions is not None:
        norm_list = [p.strip().lower() for p in positions.split(",") if p.strip()]
        if not norm_list:
            cur.close(); conn.close()
            if uploaded_new_video and video_path != old_video_path:
                _safe_remove_file(video_path)
            if uploaded_new_cover and cover_path != old_cover_path:
                _safe_remove_file(cover_path)
            return jsonify({"error": "positions is empty"}), 400

    try:
        # 1) Update bảng eln
        upd_sql = """
          UPDATE eln
          SET title=%s, positions=%s, training_time=%s, note=%s, video_path=%s, cover_path=%s, updated_at=NOW()
          WHERE id=%s
        """
        cur2 = conn.cursor()
        cur2.execute(upd_sql, (
            new_title, new_positions_str, new_training_time, new_note, video_path, cover_path, item_id
        ))
        cur2.close()

        # 2) Đồng bộ mapping theo positions mới
        pos_list = [p.strip().lower() for p in (new_positions_str or "").split(",") if p.strip()]
        if pos_list:
            placeholders = ", ".join(["%s"] * len(pos_list))
            sel_emp_sql = f"""
                SELECT id AS employee_id
                FROM nsh.employees2026_base
                WHERE employment_status = 'active'
                  AND LOWER(TRIM(vi_tri)) IN ({placeholders})
            """
            cur3 = conn.cursor(dictionary=True)
            cur3.execute(sel_emp_sql, tuple(pos_list))
            target_emps = {r["employee_id"] for r in cur3.fetchall()}
            cur3.close()
        else:
            target_emps = set()

        cur4 = conn.cursor(dictionary=True)
        cur4.execute("""
            SELECT employee_id
            FROM nsh.eln_employee_courses
            WHERE course_id = %s
        """, (item_id,))
        existing_emps = {r["employee_id"] for r in cur4.fetchall()}
        cur4.close()

        to_insert = list(target_emps - existing_emps)
        to_delete = list(existing_emps - target_emps)

        if to_delete:
            del_placeholders = ", ".join(["%s"] * len(to_delete))
            del_sql = f"""
                DELETE FROM nsh.eln_employee_courses
                WHERE course_id = %s AND employee_id IN ({del_placeholders})
            """
            cur5 = conn.cursor()
            cur5.execute(del_sql, (item_id, *to_delete))
            cur5.close()

            curSD = conn.cursor()
            curSD.execute(
                f"""
                UPDATE nsh.eln_employee_status
                SET tong_so_mon_hoc = GREATEST(COALESCE(tong_so_mon_hoc, 0) - 1, 0)
                WHERE employee_id IN ({del_placeholders})
                """,
                tuple(to_delete)
            )
            curSD.close()

        if to_insert:
            ins_sql = """
                INSERT INTO nsh.eln_employee_courses
                    (employee_id, course_id, gan_nhat, ngay, ket_qua, hien_trang, thoi_gian_yeu_cau, status, training_type, status_watch)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    eid,
                    item_id,
                    DEFAULT_GAN_NHAT,
                    None,
                    None,
                    DEFAULT_HIEN_TRANG,
                    None,
                    DEFAULT_STATUS,
                    DEFAULT_TRAINING_TYPE,
                    DEFAULT_STATUS_WATCH
                )
                for eid in to_insert
            ]
            cur6 = conn.cursor()
            cur6.executemany(ins_sql, values)
            cur6.close()

            ins_placeholders = ", ".join(["%s"] * len(to_insert))
            curSI = conn.cursor()
            curSI.execute(
                f"""
                UPDATE nsh.eln_employee_status
                SET tong_so_mon_hoc = COALESCE(tong_so_mon_hoc, 0) + 1
                WHERE employee_id IN ({ins_placeholders})
                """,
                tuple(to_insert)
            )
            curSI.close()

            curSI2 = conn.cursor(dictionary=True)
            curSI2.execute(
                f"SELECT employee_id FROM nsh.eln_employee_status WHERE employee_id IN ({ins_placeholders})",
                tuple(to_insert)
            )
            existed2 = {r["employee_id"] for r in curSI2.fetchall()}
            curSI2.close()

            missing2 = [eid for eid in to_insert if eid not in existed2]
            if missing2:
                curSI3 = conn.cursor()
                curSI3.executemany(
                    """
                    INSERT INTO nsh.eln_employee_status
                        (employee_id, hien_trang, tong_so_mon_hoc, so_mon_hoc_hoan_thanh)
                    VALUES (%s, %s, %s, %s)
                    """,
                    [(eid, None, 1, 0) for eid in missing2]
                )
                curSI3.close()

        # 2b) Tính lại counter cho môn học
        cur_cnt = conn.cursor(dictionary=True)
        cur_cnt.execute("""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN ket_qua = 'pass' THEN 1 ELSE 0 END) AS passed
            FROM nsh.eln_employee_courses
            WHERE course_id = %s
        """, (item_id,))
        cnt = cur_cnt.fetchone() or {"total": 0, "passed": 0}
        cur_cnt.close()

        cur_upd_count = conn.cursor()
        cur_upd_count.execute("""
            UPDATE eln
            SET tong_nhan_vien_hoc = %s,
                so_nhan_vien_hoan_thanh = %s
            WHERE id = %s
        """, (cnt["total"] or 0, cnt["passed"] or 0, item_id))
        cur_upd_count.close()

        conn.commit()

    except Exception:
        conn.rollback()
        if uploaded_new_video and video_path != old_video_path:
            _safe_remove_file(video_path)
        if uploaded_new_cover and cover_path != old_cover_path:
            _safe_remove_file(cover_path)
        cur.close(); conn.close()
        app.logger.exception("[ELN] Update failed (with mapping + status sync)")
        return jsonify({"error": "Update failed"}), 500

    # Commit OK -> xoá file cũ nếu có upload mới (xoá trên UNC)
    removed_old_video = False
    removed_old_cover = False
    if uploaded_new_video and old_video_path and old_video_path != video_path:
        removed_old_video = _safe_remove_file(old_video_path)
    if uploaded_new_cover and old_cover_path and old_cover_path != cover_path:
        removed_old_cover = _safe_remove_file(old_cover_path)

    cur.close(); conn.close()
    return jsonify({
        "ok": True,
        "removed_old_video": removed_old_video,
        "removed_old_cover": removed_old_cover,
        "mapping_sync": {
            "added": len(to_insert) if 'to_insert' in locals() else 0,
            "removed": len(to_delete) if 'to_delete' in locals() else 0
        }
    }), 200


@eln_bp.route("/eln", methods=["POST"])
def create_eln():
    title = request.form.get("title", "").strip()
    positions = request.form.get("positions", "")
    training_time = request.form.get("training_time") or None
    note = request.form.get("note", "")

    if not title:
        return jsonify({"error": "title is required"}), 400
    if not positions:
        return jsonify({"error": "positions is required"}), 400

    pos_list = [p.strip().lower() for p in positions.split(",") if p.strip()]
    if not pos_list:
        return jsonify({"error": "positions is empty"}), 400

    video_path = None
    cover_path = None

    # Upload file lên UNC
    if "video" in request.files:
        video_path = _save_file(request.files["video"], VIDEO_DIR, ALLOWED_VIDEO)
        if request.files["video"].filename and not video_path:
            return jsonify({"error": "Invalid video format"}), 400

    if "cover" in request.files:
        cover_path = _save_file(request.files["cover"], COVER_DIR, ALLOWED_IMAGE)
        if request.files["cover"].filename and not cover_path:
            return jsonify({"error": "Invalid image format"}), 400

    conn = get_connection()
    try:
        placeholders = ", ".join(["%s"] * len(pos_list))
        sel_sql = f"""
            SELECT id AS employee_id
            FROM nsh.employees2026_base
            WHERE employment_status = 'active'
              AND LOWER(TRIM(vi_tri)) IN ({placeholders})
        """
        cur2 = conn.cursor(dictionary=True)
        cur2.execute(sel_sql, tuple(pos_list))
        employees = cur2.fetchall()
        cur2.close()

        total_learners = len(employees) if employees else 0
        total_passed = 0

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO eln (title, positions, training_time, note, video_path, cover_path,
                             tong_nhan_vien_hoc, so_nhan_vien_hoan_thanh,
                             created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s,
                    NOW(), NOW())
        """, (title, positions, training_time, note, video_path, cover_path,
              total_learners, total_passed))
        new_id = cur.lastrowid
        cur.close()

        if employees:
            ins_sql = """
                INSERT INTO nsh.eln_employee_courses
                    (employee_id, course_id, gan_nhat, ngay, ket_qua, hien_trang, thoi_gian_yeu_cau, status, training_type, status_watch)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = []
            emp_ids = []
            for e in employees:
                eid = e["employee_id"]
                emp_ids.append(eid)
                values.append((
                    eid, new_id,
                    DEFAULT_GAN_NHAT,
                    None,
                    None,
                    DEFAULT_HIEN_TRANG,
                    None,
                    DEFAULT_STATUS,
                    DEFAULT_TRAINING_TYPE,
                    DEFAULT_STATUS_WATCH
                ))
            cur3 = conn.cursor()
            cur3.executemany(ins_sql, values)
            cur3.close()

            placeholders_emp = ", ".join(["%s"] * len(emp_ids))

            curS1 = conn.cursor()
            curS1.execute(
                f"""
                UPDATE nsh.eln_employee_status
                SET tong_so_mon_hoc = COALESCE(tong_so_mon_hoc, 0) + 1
                WHERE employee_id IN ({placeholders_emp})
                """,
                tuple(emp_ids)
            )
            curS1.close()

            curS2 = conn.cursor(dictionary=True)
            curS2.execute(
                f"SELECT employee_id FROM nsh.eln_employee_status WHERE employee_id IN ({placeholders_emp})",
                tuple(emp_ids)
            )
            existed = {r["employee_id"] for r in curS2.fetchall()}
            curS2.close()

            missing = [eid for eid in emp_ids if eid not in existed]
            if missing:
                curS3 = conn.cursor()
                curS3.executemany(
                    """
                    INSERT INTO nsh.eln_employee_status
                        (employee_id, hien_trang, tong_so_mon_hoc, so_mon_hoc_hoan_thanh)
                    VALUES (%s, %s, %s, %s)
                    """,
                    [(eid, None, 1, 0) for eid in missing]
                )
                curS3.close()

        conn.commit()
    except Exception:
        conn.rollback()
        # rollback file đã upload lên UNC nếu tạo course fail
        if video_path:
            _safe_remove_file(video_path)
        if cover_path:
            _safe_remove_file(cover_path)
        conn.close()
        app.logger.exception("[ELN] Create failed (and employee_courses mapping + status sync)")
        return jsonify({"error": "Create failed"}), 500

    conn.close()
    return jsonify({"id": new_id, "linked_employees": len(employees) if employees else 0}), 201


@eln_bp.route("/eln/<int:item_id>", methods=["DELETE"])
def delete_eln(item_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT video_path, cover_path FROM eln WHERE id=%s", (item_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Not found"}), 404

    video_path = row.get("video_path")
    cover_path = row.get("cover_path")

    curM = conn.cursor(dictionary=True)
    curM.execute("SELECT employee_id FROM nsh.eln_employee_courses WHERE course_id = %s", (item_id,))
    mapped = [r["employee_id"] for r in curM.fetchall()]
    curM.close()

    # Xoá file trên UNC trước (không fail nếu lỗi)
    removed_video = _safe_remove_file(video_path)
    removed_cover = _safe_remove_file(cover_path)

    try:
        if mapped:
            placeholders_emp = ", ".join(["%s"] * len(mapped))
            curDStat = conn.cursor()
            curDStat.execute(
                f"""
                UPDATE nsh.eln_employee_status
                SET tong_so_mon_hoc = GREATEST(COALESCE(tong_so_mon_hoc, 0) - 1, 0)
                WHERE employee_id IN ({placeholders_emp})
                """,
                tuple(mapped)
            )
            curDStat.close()

        cur2 = conn.cursor()
        cur2.execute("DELETE FROM nsh.eln_employee_courses WHERE course_id = %s", (item_id,))
        cur2.execute("DELETE FROM eln WHERE id=%s", (item_id,))
        conn.commit()
        cur2.close()
    except Exception:
        conn.rollback()
        cur.close(); conn.close()
        app.logger.exception("[ELN] Delete failed (with employee_courses cleanup + status sync)")
        return jsonify({
            "error": "Delete failed",
            "removed_video": removed_video,
            "removed_cover": removed_cover
        }), 500

    cur.close(); conn.close()
    return jsonify({
        "ok": True,
        "removed_video": removed_video,
        "removed_cover": removed_cover
    }), 200
