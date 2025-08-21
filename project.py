from flask import Blueprint, request, jsonify
from datetime import datetime
import traceback

from database import get_connection

project_bp = Blueprint('project', __name__)

def validate_date(date_str):
    if not date_str or date_str == "":
        return None
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        return None

@project_bp.route('/projects', methods=['GET'])
def get_projects():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT projectName, codeproject, note, delivery, lastWeek, nextWeek,
                   picBod, delayReasons, md, cd1, cd2, mold, sm, nsPur, spPur,
                   asproject, wr, cli, poNumber, confirm, quantity, giatri,
                   client, picSales, no
            FROM dataprojects
        """)
        projects = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(projects)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@project_bp.route('/projects/<int:no>', methods=['GET'])
def get_project(no):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT projectName, codeproject, note, delivery, lastWeek, nextWeek,
                   picBod, delayReasons, md, cd1, cd2, mold, sm, nsPur, spPur,
                   asproject, wr, cli, poNumber, confirm, quantity, giatri,
                   client, picSales, no
            FROM dataprojects WHERE no = %s
        """, (no,))
        project = cursor.fetchone()
        cursor.close()
        conn.close()
        if project:
            return jsonify(project)
        return jsonify({"error": "Project not found"}), 404
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@project_bp.route('/projects', methods=['POST'])
def add_project():
    try:
        data = request.get_json()
        required_fields = ['projectName', 'codeproject']
        missing_fields = [field for field in required_fields if not data.get(field) or data.get(field).strip() == ""]
        if missing_fields:
            return jsonify({"error": f"Missing or empty required fields: {', '.join(missing_fields)}"}), 400

        conn = get_connection()
        cursor = conn.cursor()
        query = """INSERT INTO dataprojects
                   (projectName, codeproject, note, delivery, lastWeek, nextWeek,
                    picBod, delayReasons, md, cd1, cd2, mold, sm, nsPur, spPur,
                    asproject, wr, cli, poNumber, confirm, quantity, giatri,
                    client, picSales)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (
            data.get('projectName'),
            data.get('codeproject'),
            data.get('note') or None,
            validate_date(data.get('delivery')),
            data.get('lastWeek') or None,
            data.get('nextWeek') or None,
            data.get('picBod') or None,
            data.get('delayReasons') or None,
            data.get('md') or None,
            data.get('cd1') or None,
            data.get('cd2') or None,
            data.get('mold') or None,
            data.get('sm') or None,
            data.get('nsPur') or None,
            data.get('spPur') or None,
            data.get('asproject') or None,
            data.get('wr') or None,
            data.get('cli') or None,
            data.get('poNumber') or None,
            data.get('confirm'),
            data.get('quantity'),
            data.get('giatri'),
            data.get('client') or None,
            data.get('picSales') or None
        )

        cursor.execute(query, values)
        conn.commit()
        new_id = cursor.lastrowid
        cursor.close()
        conn.close()
        return jsonify({"message": "Project added successfully", "no": new_id}), 201
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@project_bp.route('/projects/<int:no>', methods=['PUT'])
def update_project(no):
    try:
        data = request.get_json()
        conn = get_connection()
        cursor = conn.cursor()

        query = """UPDATE dataprojects
                   SET projectName = %s,
                       codeproject = %s,
                       note = %s,
                       delivery = %s,
                       lastWeek = %s,
                       nextWeek = %s,
                       picBod = %s,
                       delayReasons = %s,
                       md = %s,
                       cd1 = %s,
                       cd2 = %s,
                       mold = %s,
                       sm = %s,
                       nsPur = %s,
                       spPur = %s,
                       asproject = %s,
                       wr = %s,
                       cli = %s,
                       poNumber = %s,
                       confirm = %s,
                       quantity = %s,
                       giatri = %s,
                       client = %s,
                       picSales = %s
                   WHERE no = %s"""
        values = (
            data.get('projectName'),
            data.get('codeproject'),
            data.get('note') or None,
            validate_date(data.get('delivery')),
            data.get('lastWeek') or None,
            data.get('nextWeek') or None,
            data.get('picBod') or None,
            data.get('delayReasons') or None,
            data.get('md') or None,
            data.get('cd1') or None,
            data.get('cd2') or None,
            data.get('mold') or None,
            data.get('sm') or None,
            data.get('nsPur') or None,
            data.get('spPur') or None,
            data.get('asproject') or None,
            data.get('wr') or None,
            data.get('cli') or None,
            data.get('poNumber') or None,
            data.get('confirm'),
            data.get('quantity'),
            data.get('giatri'),
            data.get('client') or None,
            data.get('picSales') or None,
            no
        )

        cursor.execute(query, values)
        conn.commit()
        affected = cursor.rowcount
        cursor.close()
        conn.close()
        if affected == 0:
            return jsonify({"error": "Project not found"}), 404
        return jsonify({"message": "Project updated successfully"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@project_bp.route('/projects/<int:no>', methods=['DELETE'])
def delete_project(no):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT no FROM dataprojects WHERE no = %s", (no,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({"error": "Project not found"}), 404

        cursor.execute("DELETE FROM dataprojects WHERE no = %s", (no,))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Project deleted successfully"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
