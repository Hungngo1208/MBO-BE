# main.py
from flask import Flask, send_from_directory, abort
from flask_cors import CORS
import os

# ==== Import các Blueprint hiện có ====
from auth import auth_bp
from project import project_bp
from permission.project_permission import permission_bp
from employees import employees_bp
from department import department_bp
from MBO.personalMBO import employees_bpp
from MBO.competencyMBO import competency_bp
from MBO.allocationsMBO import allocations_bp
from permission.roles import roles_bp
from MBO.submit import submit_bp
from MBO.timelineMBO import mbo_timeline_bpp, ensure_table
from MBO.status import status_bp
from MBO.attitudeMBO import attitude_bp
from MBO.mbo_notifications import mbo_notifications_bp

from ELearning.eln import eln_bp
from ELearning.eln_employee_list import eln_employee_bp
from ELearning.eln_request import eln_request_bp
from ELearning.eln_courses import eln_courses_bp
from ELearning.quizz import bp as quiz_bp
from personnel_notifications import personnel_notifications_bp
from employees_notifications import employees_notifications_bp
# ==== Khởi tạo Flask ====
app = Flask(__name__)
CORS(app)

# ==== Đăng ký Blueprints ====
app.register_blueprint(auth_bp)
app.register_blueprint(project_bp)
app.register_blueprint(employees_bp)
app.register_blueprint(department_bp)
app.register_blueprint(permission_bp)
app.register_blueprint(employees_bpp)
app.register_blueprint(competency_bp)
app.register_blueprint(allocations_bp)
app.register_blueprint(roles_bp)
app.register_blueprint(submit_bp)
app.register_blueprint(mbo_timeline_bpp)
app.register_blueprint(status_bp)
app.register_blueprint(attitude_bp, url_prefix="/attitude")
app.register_blueprint(eln_bp)
app.register_blueprint(eln_employee_bp)
app.register_blueprint(eln_request_bp)
app.register_blueprint(eln_courses_bp)
app.register_blueprint(quiz_bp)
app.register_blueprint(personnel_notifications_bp)
app.register_blueprint(mbo_notifications_bp)
app.register_blueprint(employees_notifications_bp)
# ==== Đảm bảo bảng timeline tồn tại ====
with app.app_context():
    ensure_table()

# ============================================================
# MEDIA ROOT: LUÔN LẤY FILE Ở FILE SERVER (UNC)
# ============================================================
MEDIA_ROOT = r"\\10.73.131.2\eln_media"
# Nếu bạn thật sự có share name "media" thì đổi thành:
# MEDIA_ROOT = r"\\10.73.131.2\media"


def _normalize_rel_path(p: str) -> str:
    """
    Chuẩn hoá path từ request:
    - bỏ leading '/'
    - đổi '\' -> '/'
    - chặn traversal '..'
    - hỗ trợ legacy: 'uploads/eln/...'
    """
    p = (p or "").replace("\\", "/").lstrip("/")

    # Tương thích cũ: DB/FE có thể gửi uploads/eln/videos/xxx.mp4
    if p.startswith("uploads/eln/"):
        p = p[len("uploads/eln/"):]

    # Chặn path traversal
    if ".." in p.split("/"):
        abort(400)

    return p


def _send_from_media(rel_path: str):
    """
    Send file từ UNC media root.
    rel_path ví dụ:
      - videos/abc.mp4
      - covers/xyz.png
    """
    rel = _normalize_rel_path(rel_path)
    fullpath = os.path.join(MEDIA_ROOT, rel.replace("/", os.sep))

    if not os.path.isfile(fullpath):
        abort(404)

    directory = os.path.dirname(fullpath)
    base = os.path.basename(fullpath)

    resp = send_from_directory(directory, base, conditional=True)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Accept-Ranges"] = "bytes"
    return resp


# ===== Route chuẩn: /uploads/... =====
@app.get("/uploads/<path:filename>")
def serve_uploads(filename):
    return _send_from_media(filename)


# ===== Alias cho frontend đang gọi /covers/... và /videos/... =====
@app.get("/covers/<path:filename>")
def serve_covers(filename):
    return _send_from_media(f"covers/{filename}")


@app.get("/videos/<path:filename>")
def serve_videos(filename):
    return _send_from_media(f"videos/{filename}")


# ==== Chạy server ====
if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)
