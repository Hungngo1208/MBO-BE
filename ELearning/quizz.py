# quiz.py
from flask import Blueprint, request, jsonify, make_response
from typing import Dict, Any, List, Optional
from mysql.connector import Error
from database import get_connection

bp = Blueprint("eln_quiz", __name__, url_prefix="/eln")

# ========= Helpers =========
def _row_to_bool(v):
    return bool(v) if v is not None else False

def _row_to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _corsify(resp):
    """Thêm CORS header tối thiểu cho FE gọi trực tiếp."""
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

# ========= Core =========
def _fetch_quiz(course_id: int) -> Optional[Dict[str, Any]]:
    """
    Đọc quiz theo course_id và trả về JSON đúng format UI cần.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT * FROM quizzes WHERE course_id = %s LIMIT 1", (course_id,))
        quiz = cur.fetchone()
        if not quiz:
            return None

        quiz_id = quiz["id"]

        cur.execute(
            """
            SELECT id, text, allow_multi, points, ordinal
            FROM quiz_questions
            WHERE quiz_id = %s
            ORDER BY ordinal
            """,
            (quiz_id,),
        )
        questions = cur.fetchall()

        q_json: List[Dict[str, Any]] = []
        if questions:
            qids = [row["id"] for row in questions]
            in_clause = ",".join(["%s"] * len(qids))
            cur.execute(
                f"""
                SELECT id, question_id, text, is_correct, ordinal
                FROM quiz_options
                WHERE question_id IN ({in_clause})
                ORDER BY question_id, ordinal
                """,
                qids,
            )
            options = cur.fetchall() or []
            # group options by question_id
            opts_by_q: Dict[int, List[Dict[str, Any]]] = {}
            for o in options:
                opts_by_q.setdefault(o["question_id"], []).append(
                    {
                        "id": o["id"],
                        "text": o["text"],
                        "is_correct": _row_to_bool(o["is_correct"]),
                    }
                )

            for q in questions:
                q_json.append(
                    {
                        "id": q["id"],
                        "text": q["text"],
                        "allow_multi": _row_to_bool(q["allow_multi"]),
                        "points": _row_to_float(q["points"]) or 1.0,
                        "options": opts_by_q.get(q["id"], []),
                    }
                )
        result = {
            "id": quiz_id,
            "course_id": quiz["course_id"],
            "title": quiz["title"],
            "time_limit": quiz.get("time_limit_min"),
            "pass_score": _row_to_float(quiz.get("pass_score")),
            "shuffle_questions": _row_to_bool(quiz.get("shuffle_questions")),
            "shuffle_options": _row_to_bool(quiz.get("shuffle_options")),
            "version": quiz.get("version", 1),
            "questions": q_json,
        }
        return result
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ========= Routes: get / put =========
@bp.get("/<int:course_id>/quiz")
def get_quiz(course_id: int):
    """
    Lấy quiz theo course_id. Nếu chưa có trả về {} để UI hiển thị form trống.
    """
    try:
        data = _fetch_quiz(course_id)
        resp = jsonify({} if not data else data)
        return _corsify(resp), 200
    except Exception as e:
        return _corsify(jsonify({"ok": False, "error": str(e)})), 500

@bp.put("/<int:course_id>/quiz")
def upsert_quiz(course_id: int):
    """
    Lưu quiz theo payload UI gửi (xoá & chèn lại question/option theo ordinal).
    Trả về { ok, quiz_id, version }.
    """
    body = request.get_json(silent=True) or {}
    title = (body.get("title") or "").strip()
    if not title:
        return _corsify(jsonify({"ok": False, "error": "title_required"})), 400

    time_limit = body.get("time_limit", None)  # phút hoặc None
    pass_score = body.get("pass_score", None)  # float hoặc None
    shuffle_questions = bool(body.get("shuffle_questions", True))
    shuffle_options = bool(body.get("shuffle_options", True))
    questions: List[Dict[str, Any]] = body.get("questions") or []

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(dictionary=True)
    try:
        # 1) Tìm quiz theo course_id
        cur.execute("SELECT id, version FROM quizzes WHERE course_id = %s LIMIT 1", (course_id,))
        row = cur.fetchone()

        if row:
            quiz_id = row["id"]
            # update meta + tăng version
            cur.execute(
                """
                UPDATE quizzes
                SET title = %s,
                    time_limit_min = %s,
                    pass_score = %s,
                    shuffle_questions = %s,
                    shuffle_options = %s,
                    version = version + 1,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (title, time_limit, pass_score, shuffle_questions, shuffle_options, quiz_id),
            )
            # Xoá options & questions cũ
            cur.execute("SELECT id FROM quiz_questions WHERE quiz_id = %s", (quiz_id,))
            qids = [r["id"] for r in (cur.fetchall() or [])]
            if qids:
                in_clause = ",".join(["%s"] * len(qids))
                cur.execute(f"DELETE FROM quiz_options WHERE question_id IN ({in_clause})", qids)
            cur.execute("DELETE FROM quiz_questions WHERE quiz_id = %s", (quiz_id,))
        else:
            # insert quiz mới
            cur.execute(
                """
                INSERT INTO quizzes (course_id, title, time_limit_min, pass_score, shuffle_questions, shuffle_options)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (course_id, title, time_limit, pass_score, shuffle_questions, shuffle_options),
            )
            quiz_id = cur.lastrowid

        # 2) Chèn lại questions/options theo thứ tự mảng
        for i, q in enumerate(questions):
            q_text = (q.get("text") or "").strip()
            q_allow_multi = bool(q.get("allow_multi"))
            q_points = q.get("points", 1)
            try:
                q_points = float(q_points)
            except Exception:
                q_points = 1.0

            cur.execute(
                """
                INSERT INTO quiz_questions (quiz_id, ordinal, text, allow_multi, points)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (quiz_id, i, q_text, q_allow_multi, q_points),
            )
            question_id = cur.lastrowid

            options = q.get("options") or []
            for j, o in enumerate(options):
                o_text = (o.get("text") or "").strip()
                o_correct = bool(o.get("is_correct"))
                cur.execute(
                    """
                    INSERT INTO quiz_options (question_id, ordinal, text, is_correct)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (question_id, j, o_text, o_correct),
                )

        # 3) Lấy version hiện tại để trả về
        cur.execute("SELECT version FROM quizzes WHERE id = %s", (quiz_id,))
        version = (cur.fetchone() or {}).get("version", 1)

        conn.commit()
        return _corsify(jsonify({"ok": True, "quiz_id": quiz_id, "version": version})), 200
    except Exception as e:
        conn.rollback()
        return _corsify(jsonify({"ok": False, "error": str(e)})), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ========= CORS preflight cho submit =========
@bp.post("/<int:course_id>/quiz/submit")
def submit_quiz_result(course_id: int):
    """
    Nhận kết quả nộp bài cho một employee trong một course và cập nhật:
      - CHÈN 1 dòng lịch sử vào nsh.eln_quiz_submissions (luôn chèn)
      - UPDATE nsh.eln_employee_courses: ket_qua, status, ngay
        * Nếu chuyển fail -> pass: +1 vào nsh.eln_employee_status.so_mon_hoc_hoan_thanh
        * Nếu pass -> fail: KHÔNG update dòng tổng hợp (vẫn giữ pass)
        * Nếu lần nộp này fail và status cũ cũng là fail -> status_watch = 'fail'
    """
    body = request.get_json(silent=True) or {}
    employee_id = body.get("employee_id", None)
    status = (body.get("status") or "").strip().lower()
    ket_qua = body.get("ket_qua", None)

    if not employee_id:
        return _corsify(jsonify({"ok": False, "error": "employee_id_required"})), 400
    if status not in ("pass", "fail"):
        return _corsify(jsonify({"ok": False, "error": "invalid_status", "hint": "status must be 'pass' or 'fail'"})), 400

    ket_qua_str = None if ket_qua is None else str(ket_qua)

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(dictionary=True)
    try:
        # 1) Kiểm tra mapping tồn tại
        cur.execute(
            """
            SELECT id, status, ket_qua, status_watch
            FROM nsh.eln_employee_courses
            WHERE employee_id = %s AND course_id = %s
            LIMIT 1
            """,
            (employee_id, course_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return _corsify(jsonify({
                "ok": False,
                "error": "not_found",
                "message": "Không tìm thấy bản ghi trong nsh.eln_employee_courses cho employee_id & course_id"
            })), 404

        prev_status = (row.get("status") or "").strip().lower()
        prev_ket_qua = row.get("ket_qua")
        prev_status_watch = (row.get("status_watch") or "").strip().lower()

        transitioned_fail_to_pass = (prev_status == "fail" and status == "pass")
        skipped_update_due_to_pass_protection = False

        # NEW: nếu lần nộp này fail và status cũ cũng là fail -> sẽ set status_watch = 'fail'
        mark_status_watch_fail = (status == "fail" and prev_status == "fail")

        # 2) CHÈN LỊCH SỬ NỘP BÀI (luôn chèn)
        cur.execute(
            """
            INSERT INTO nsh.eln_quiz_submissions
                (employee_id, course_id, status, ket_qua, submitted_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (employee_id, course_id, status, ket_qua_str),
        )

        # 3) CẬP NHẬT BẢNG TỔNG HỢP
        if prev_status == "pass" and status == "fail":
            # Không hạ trạng thái pass xuống fail
            skipped_update_due_to_pass_protection = True
            changed = False
        else:
            if status == "pass":
                cur.execute(
                    """
                    UPDATE nsh.eln_employee_courses
                       SET ket_qua = %s,
                           status = %s,
                           gan_nhat = 'Đã đào tạo',
                           hien_trang = 'Đã hoàn thành',
                           ngay = CURDATE()
                     WHERE employee_id = %s
                       AND course_id = %s
                    """,
                    (ket_qua_str, "pass", employee_id, course_id),
                )
            else:
                # status == "fail"
                if mark_status_watch_fail:
                    # Lần này fail và trước đó cũng fail -> đánh dấu cần học lại
                    cur.execute(
                        """
                        UPDATE nsh.eln_employee_courses
                           SET ket_qua = %s,
                               status = %s,
                               status_watch = 'fail',
                               ngay = CURDATE()
                         WHERE employee_id = %s
                           AND course_id = %s
                        """,
                        (ket_qua_str, "fail", employee_id, course_id),
                    )
                else:
                    # Các trường hợp fail khác (ví dụ từ draft/null -> fail)
                    cur.execute(
                        """
                        UPDATE nsh.eln_employee_courses
                           SET ket_qua = %s,
                               status = %s,
                               ngay = CURDATE()
                         WHERE employee_id = %s
                           AND course_id = %s
                        """,
                        (ket_qua_str, "fail", employee_id, course_id),
                    )

            # changed = có thay đổi status, ket_qua hoặc status_watch (trong case fail->fail)
            changed = (
                (prev_status != status)
                or ((prev_ket_qua or None) != ket_qua_str)
                or (mark_status_watch_fail and prev_status_watch != "fail")
            )

        # 4) Nếu chuyển fail -> pass: +1 số môn hoàn thành & số nhân viên hoàn thành
        if transitioned_fail_to_pass:
            cur.execute(
                """
                UPDATE nsh.eln_employee_status
                   SET so_mon_hoc_hoan_thanh = COALESCE(so_mon_hoc_hoan_thanh, 0) + 1
                 WHERE employee_id = %s
                """,
                (employee_id,),
            )
            cur.execute(
                """
                UPDATE nsh.eln
                   SET so_nhan_vien_hoan_thanh = COALESCE(so_nhan_vien_hoan_thanh, 0) + 1
                 WHERE id = %s
                """,
                (course_id,),
            )

        conn.commit()

        # 5) Trả dữ liệu mới nhất trong eln_employee_courses
        cur.execute(
            """
            SELECT id, employee_id, course_id, gan_nhat, ngay, ket_qua, hien_trang,
                   thoi_gian_yeu_cau, status, training_type, status_watch
            FROM nsh.eln_employee_courses
            WHERE employee_id = %s AND course_id = %s
            LIMIT 1
            """,
            (employee_id, course_id),
        )
        updated = cur.fetchone() or {}

        # 6) Lần nộp mới nhất từ lịch sử
        cur.execute(
            """
            SELECT id, status, ket_qua, submitted_at
            FROM nsh.eln_quiz_submissions
            WHERE employee_id = %s AND course_id = %s
            ORDER BY submitted_at DESC, id DESC
            LIMIT 1
            """,
            (employee_id, course_id),
        )
        last_submission = cur.fetchone()

        return _corsify(jsonify({
            "ok": True,
            "data": updated,
            "last_submission": last_submission,
            "meta": {
                "changed": bool(changed),
                "no_change": not changed,
                "incremented_so_mon_hoc_hoan_thanh": bool(transitioned_fail_to_pass),
                "skipped_update_due_to_pass_protection": bool(skipped_update_due_to_pass_protection)
            }
        })), 200

    except Error as db_err:
        conn.rollback()
        return _corsify(jsonify({"ok": False, "error": "db_error", "message": str(db_err)})), 500
    except Exception as e:
        conn.rollback()
        return _corsify(jsonify({"ok": False, "error": str(e)})), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

