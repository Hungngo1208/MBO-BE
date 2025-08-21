from flask import Flask
from flask_cors import CORS

from auth import auth_bp
from project import project_bp
from permission.project_permission import permission_bp
from employees import employees_bp
from department import department_bp  # <-- Thêm dòng này nếu bạn tách cây tổ chức
from MBO.personalMBO import employees_bpp
from MBO.competencyMBO import competency_bp
from MBO.allocationsMBO import allocations_bp
from permission.roles import roles_bp
from MBO.submit import submit_bp
app = Flask(__name__)
CORS(app)

# Register all Blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(project_bp)
app.register_blueprint(employees_bp)     # danh sách nhân viên
app.register_blueprint(department_bp)    # cây cơ cấu tổ chức (từ organization_units)
app.register_blueprint(permission_bp)
app.register_blueprint(employees_bpp)    # MBO cá nhân
app.register_blueprint(competency_bp)
app.register_blueprint(allocations_bp)
app.register_blueprint(roles_bp)
app.register_blueprint(submit_bp)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
