# eln.py
import os
import uuid
from flask import Blueprint, request, jsonify, send_from_directory
from flask import current_app as app
from werkzeug.utils import secure_filename
from database import get_connection

eln_bp = Blueprint("eln", __name__)

# ========== Cấu hình thư mục lưu file ==========
BASE_UPLOAD = os.getenv("ELN_UPLOAD_DIR", os.path.join(os.getcwd(), "uploads", "eln"))
VIDEO_DIR = os.path.join(BASE_UPLOAD, "videos")
COVER_DIR = os.path.join(BASE_UPLOAD, "covers")
os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(COVER_DIR, exist_ok=True)

ALLOWED_IMAGE = {"png", "jpg", "jpeg", "gif", "webp"}
ALLOWED_VIDEO = {"mp4", "mov", "avi", "mkv", "webm"}


def _ext_ok(filename, allow_set):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in allow_set


def _save_file(file_storage, folder, allow_set):
    """
    Lưu file vào thư mục 'folder' với tên UUID, trả về đường dẫn tương đối từ cwd để lưu DB.
    """
    if not file_storage or file_storage.filename == "":
        return None
    fname = secure_filename(file_storage.filename)
    if not _ext_ok(fname, allow_set):
        return None
    ext = fname.rsplit(".", 1)[1].lower()
    new_name = f"{uuid.uuid4().hex}.{ext}"
    path = os.path.join(folder, new_name)
    file_storage.save(path)
    # Trả về đường dẫn tương đối để lưu DB (dạng: uploads/eln/videos/xxx.mp4)
    rel = os.path.relpath(path, os.getcwd()).replace("\\", "/")
    return rel


def _abs_from_rel(rel_path: str):
    """
    Convert đường dẫn tương đối (lưu trong DB) -> absolute path trên server.
    An toàn vì tên file do hệ thống sinh (UUID).
    """
    if not rel_path:
        return None
    # Chuẩn hoá
    rp = rel_path.replace("\\", "/")
    # Nếu đã là absolute
    if os.path.isabs(rp):
        return rp
    # Ghép với cwd
    abs_path = os.path.join(os.getcwd(), rp)
    return abs_path


def _safe_remove_file(rel_path: str) -> bool:
    """
    Xoá file theo đường dẫn tương đối lưu trong DB. Không raise exception.
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


# ========== FILE SERVE (tuỳ chọn) ==========
@eln_bp.route("/files/eln/videos/<path:filename>")
def serve_video(filename):
    return send_from_directory(VIDEO_DIR, filename, as_attachment=False)


@eln_bp.route("/files/eln/covers/<path:filename>")
def serve_cover(filename):
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
          training_time,             -- giữ nguyên, có thể là TEXT
          note,
          video_path,
          cover_path,
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
          training_time,             -- giữ nguyên, có thể là TEXT
          note,
          video_path,
          cover_path,
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


# POST /eln -> thêm mới (multipart/form-data)
@eln_bp.route("/eln", methods=["POST"])
def create_eln():
    title = request.form.get("title", "").strip()
    positions = request.form.get("positions", "")              # ví dụ "staff,ld"
    training_time = request.form.get("training_time") or None  # text theo yêu cầu
    note = request.form.get("note", "")

    if not title:
        return jsonify({"error": "title is required"}), 400
    if not positions:
        return jsonify({"error": "positions is required"}), 400

    video_path = None
    cover_path = None

    # File upload
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
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO eln (title, positions, training_time, note, video_path, cover_path, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, (title, positions, training_time, note, video_path, cover_path))
        conn.commit()
        new_id = cur.lastrowid
        cur.close()
    except Exception as e:
        conn.rollback()
        # Nếu lỗi và đã upload file, nên xoá để tránh rác
        if video_path:
            _safe_remove_file(video_path)
        if cover_path:
            _safe_remove_file(cover_path)
        conn.close()
        app.logger.exception("[ELN] Create failed")
        return jsonify({"error": "Create failed"}), 500

    conn.close()
    return jsonify({"id": new_id}), 201


# PUT /eln/<id> -> cập nhật; cho phép cập nhật file (nếu tải lên mới)
@eln_bp.route("/eln/<int:item_id>", methods=["PUT"])
def update_eln(item_id):
    title = request.form.get("title", "").strip()
    positions = request.form.get("positions")
    training_time = request.form.get("training_time") or None  # text
    note = request.form.get("note")

    # lấy bản ghi hiện tại
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM eln WHERE id=%s", (item_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Not found"}), 404

    old_video_path = row.get("video_path")
    old_cover_path = row.get("cover_path")

    # Mặc định dùng lại path cũ
    video_path = old_video_path
    cover_path = old_cover_path

    # Cờ nhận biết có upload mới
    uploaded_new_video = False
    uploaded_new_cover = False

    # Nhận & lưu video mới (nếu có)
    if "video" in request.files and request.files["video"].filename:
        new_video = _save_file(request.files["video"], VIDEO_DIR, ALLOWED_VIDEO)
        if not new_video:
            cur.close(); conn.close()
            return jsonify({"error": "Invalid video format"}), 400
        video_path = new_video
        uploaded_new_video = True

    # Nhận & lưu cover mới (nếu có)
    if "cover" in request.files and request.files["cover"].filename:
        new_cover = _save_file(request.files["cover"], COVER_DIR, ALLOWED_IMAGE)
        if not new_cover:
            cur.close(); conn.close()
            return jsonify({"error": "Invalid image format"}), 400
        cover_path = new_cover
        uploaded_new_cover = True

    # Cập nhật DB
    upd_sql = """
      UPDATE eln
      SET title=%s, positions=%s, training_time=%s, note=%s, video_path=%s, cover_path=%s, updated_at=NOW()
      WHERE id=%s
    """
    try:
        cur2 = conn.cursor()
        cur2.execute(upd_sql, (
            title or row["title"],
            positions if positions is not None else row["positions"],
            training_time if training_time is not None else row["training_time"],
            note if note is not None else row["note"],
            video_path,
            cover_path,
            item_id
        ))
        conn.commit()
        cur2.close()
    except Exception as e:
        # Rollback nếu lỗi; xoá file mới vừa upload (nếu có) để không rác
        conn.rollback()
        if uploaded_new_video and video_path != old_video_path:
            _safe_remove_file(video_path)
        if uploaded_new_cover and cover_path != old_cover_path:
            _safe_remove_file(cover_path)
        cur.close(); conn.close()
        app.logger.exception("[ELN] Update failed")
        return jsonify({"error": "Update failed"}), 500

    # Commit OK -> xoá file cũ nếu có upload mới
    removed_old_video = False
    removed_old_cover = False
    if uploaded_new_video and old_video_path and old_video_path != video_path:
        removed_old_video = _safe_remove_file(old_video_path)
    if uploaded_new_cover and old_cover_path and old_cover_path != cover_path:
        removed_old_cover = _safe_remove_file(old_cover_path)

    cur.close(); conn.close()
    return jsonify({"ok": True, "removed_old_video": removed_old_video, "removed_old_cover": removed_old_cover}), 200


# DELETE /eln/<id> -> xoá
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

    # Xoá file trên đĩa (không làm fail nếu lỗi)
    removed_video = _safe_remove_file(video_path)
    removed_cover = _safe_remove_file(cover_path)

    try:
        cur2 = conn.cursor()
        cur2.execute("DELETE FROM eln WHERE id=%s", (item_id,))
        conn.commit()
        cur2.close()
    except Exception as e:
        conn.rollback()
        cur.close(); conn.close()
        app.logger.exception("[ELN] Delete row failed")
        return jsonify({"error": "Delete failed", "removed_video": removed_video, "removed_cover": removed_cover}), 500

    cur.close(); conn.close()
    return jsonify({"ok": True, "removed_video": removed_video, "removed_cover": removed_cover}), 200
