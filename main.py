from flask import Flask
from flask_cors import CORS

from auth import auth_bp
from project import project_bp
from permission.project_permission import permission_bp
from employees import employees_bp
from department import department_bp  # cây cơ cấu tổ chức (organization_units)
from MBO.personalMBO import employees_bpp
from MBO.competencyMBO import competency_bp
from MBO.allocationsMBO import allocations_bp
from permission.roles import roles_bp
from MBO.submit import submit_bp
from MBO.timelineMBO import mbo_timeline_bpp, ensure_table  # <-- import ensure_table
from MBO.status import status_bp
from MBO.attitudeMBO import attitude_bp
from ELearning.eln import eln_bp
app = Flask(__name__)
CORS(app)

# Register Blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(project_bp)
app.register_blueprint(employees_bp)       # danh sách nhân viên
app.register_blueprint(department_bp)      # cây cơ cấu tổ chức
app.register_blueprint(permission_bp)
app.register_blueprint(employees_bpp)      # MBO cá nhân
app.register_blueprint(competency_bp)
app.register_blueprint(allocations_bp)
app.register_blueprint(roles_bp)
app.register_blueprint(submit_bp)
app.register_blueprint(mbo_timeline_bpp)   # MBO timeline (5 phase)
app.register_blueprint(status_bp)
app.register_blueprint(attitude_bp, url_prefix="/attitude")
app.register_blueprint(eln_bp)
# Đảm bảo bảng timeline tồn tại ngay khi app khởi động
with app.app_context():
    ensure_table()

if __name__ == "__main__":
    # debug=True để auto-reload khi dev; host:0.0.0.0 cho phép truy cập LAN
    app.run(debug=True, host="0.0.0.0", port=5000)
