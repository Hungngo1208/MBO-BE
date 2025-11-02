# main.py
from flask import Flask, send_from_directory, abort
from flask_cors import CORS
from werkzeug.utils import safe_join
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
from ELearning.eln import eln_bp
from ELearning.eln_employee_list import eln_employee_bp
from ELearning.eln_request import eln_request_bp
from ELearning.eln_courses import eln_courses_bp
from ELearning.quizz import bp as quiz_bp
# ==== Khởi tạo Flask ====
app = Flask(__name__)
CORS(app)  # Cho phép FE gọi API từ domain khác (React port 3000)

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
# ==== Đảm bảo bảng timeline tồn tại ====
with app.app_context():
    ensure_table()

# ==== Phục vụ file tĩnh trong thư mục "uploads" ====
UPLOAD_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "uploads"))

@app.get("/uploads/<path:filename>")
def serve_uploads(filename):
    """Trả về ảnh/video từ thư mục uploads"""
    fullpath = safe_join(UPLOAD_ROOT, filename)
    if not fullpath or not os.path.isfile(fullpath):
        abort(404)
    resp = send_from_directory(UPLOAD_ROOT, filename, conditional=True)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Accept-Ranges"] = "bytes"
    return resp

# ==== Chạy server ====
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
