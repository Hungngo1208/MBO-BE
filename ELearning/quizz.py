# quiz.py
from flask import Blueprint, request, jsonify
from database import get_connection
from typing import Dict, Any, List, Optional
import random
import json

bp = Blueprint("eln_quiz", __name__, url_prefix="/eln")


def _row_to_bool(v):
    # mysql returns 0/1 or None
    return bool(v) if v is not None else False


def _row_to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


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


@bp.get("/<int:course_id>/quiz")
def get_quiz(course_id: int):
    """
    Lấy quiz theo course_id. Nếu chưa có trả về {} để UI hiển thị form trống.
    """
    try:
        data = _fetch_quiz(course_id)
        if not data:
            return jsonify({}), 200
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.put("/<int:course_id>/quiz")
def upsert_quiz(course_id: int):
    """
    Lưu quiz theo payload UI gửi (xoá & chèn lại question/option theo ordinal).
    Trả về { ok, quiz_id, version }.
    """
    body = request.get_json(silent=True) or {}
    title = (body.get("title") or "").strip()
    if not title:
        return jsonify({"ok": False, "error": "title_required"}), 400

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
            # Trước khi ghi mới: xoá options & questions cũ (an toàn ngay cả khi chưa set ON DELETE CASCADE)
            cur.execute(
                "SELECT id FROM quiz_questions WHERE quiz_id = %s",
                (quiz_id,),
            )
            qids = [r["id"] for r in cur.fetchall() or []]
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
        return jsonify({"ok": True, "quiz_id": quiz_id, "version": version}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
def _get_quiz_meta(quiz_id: int, cur) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, time_limit_min, pass_score
        FROM quizzes
        WHERE id = %s
        LIMIT 1
        """,
        (quiz_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "time_limit_min": row.get("time_limit_min"),
        "pass_score": _row_to_float(row.get("pass_score")),
    }


def _get_quiz_structure(quiz_id: int, cur) -> Dict[str, Any]:
    """
    Lấy toàn bộ câu hỏi/đáp án của quiz để chấm điểm.
    Trả về:
      {
        questions: { qid: {"allow_multi": bool, "points": float, "correct_ids": set[str]} },
        all_qids: set[int]
      }
    """
    cur.execute(
        """
        SELECT id, allow_multi, points
        FROM quiz_questions
        WHERE quiz_id = %s
        """,
        (quiz_id,),
    )
    rows = cur.fetchall() or []
    questions = {}
    all_qids = set()
    if rows:
        qids = [r["id"] for r in rows]
        for r in rows:
            questions[r["id"]] = {
                "allow_multi": _row_to_bool(r["allow_multi"]),
                "points": _row_to_float(r["points"]) or 1.0,
                "correct_ids": set(),
            }
            all_qids.add(r["id"])

        in_clause = ",".join(["%s"] * len(qids))
        cur.execute(
            f"""
            SELECT id, question_id, is_correct
            FROM quiz_options
            WHERE question_id IN ({in_clause})
            """,
            qids,
        )
        opts = cur.fetchall() or []
        for o in opts:
            if _row_to_bool(o["is_correct"]):
                q = questions.get(o["question_id"])
                if q:
                    q["correct_ids"].add(str(o["id"]))

    return {"questions": questions, "all_qids": all_qids}


def _grade_attempt(quiz_struct: Dict[str, Any], answers_in: List[Dict[str, Any]]):
    """
    Chấm điểm dựa trên cấu trúc quiz + câu trả lời từ FE.
    answers_in: [{question_id, selected_option_ids: [..]}]
    Trả về:
      total_score: float,
      per_question: [{question_id, is_correct, earned_points, selected_option_ids(list[str])}]
    """
    qmap = quiz_struct["questions"]
    total = 0.0
    perq = []

    # Map nhanh answer theo qid
    in_by_q = {}
    for a in answers_in or []:
        qid = int(a.get("question_id"))
        sel = a.get("selected_option_ids") or []
        # chuẩn hoá về chuỗi để so sánh với correct_ids (str)
        sel_norm = sorted([str(x) for x in sel])
        in_by_q[qid] = sel_norm

    for qid, qinfo in qmap.items():
        chosen = in_by_q.get(qid, [])
        correct_sorted = sorted(qinfo["correct_ids"])
        is_ok = (chosen == correct_sorted)
        earned = qinfo["points"] if is_ok else 0.0
        total += earned
        perq.append({
            "question_id": qid,
            "is_correct": is_ok,
            "earned_points": earned,
            "selected_option_ids": chosen,
        })

    return total, perq


# -------- Routes --------

@bp.post("/quizzes/attempt/start")
def attempt_start():
    """
    Body: { quiz_id: int, employee_id: int }
    Trả về: { attempt_id, time_limit_sec }
    """
    body = request.get_json(silent=True) or {}
    quiz_id = body.get("quiz_id")
    employee_id = body.get("employee_id")

    if not quiz_id or not employee_id:
        return jsonify({"ok": False, "error": "quiz_id_and_employee_id_required"}), 400

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(dictionary=True)
    try:
        meta = _get_quiz_meta(int(quiz_id), cur)
        if not meta:
            return jsonify({"ok": False, "error": "quiz_not_found"}), 404

        # seed để backend có thể xáo trộn nếu muốn (hiện tại FE đã shuffle)
        seed = random.randint(1, 10_000_000)

        cur.execute(
            """
            INSERT INTO quiz_attempts (quiz_id, employee_id, seed)
            VALUES (%s, %s, %s)
            """,
            (quiz_id, employee_id, seed),
        )
        attempt_id = cur.lastrowid
        conn.commit()

        time_limit_sec = int(meta["time_limit_min"] or 0) * 60
        return jsonify({"ok": True, "attempt_id": attempt_id, "time_limit_sec": time_limit_sec}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@bp.post("/quizzes/attempt/submit")
def attempt_submit():
    """
    Body: {
      attempt_id: int,
      answers: [{ question_id, selected_option_ids: [id1, id2, ...] }],
      auto_submit: bool
    }
    Trả về: { ok, score, passed }
    """
    body = request.get_json(silent=True) or {}
    attempt_id = body.get("attempt_id")
    answers_in = body.get("answers") or []
    auto_submit = bool(body.get("auto_submit", False))

    if not attempt_id:
        return jsonify({"ok": False, "error": "attempt_id_required"}), 400

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(dictionary=True)
    try:
        # 1) Lấy thông tin attempt + quiz meta
        cur.execute(
            """
            SELECT a.id, a.quiz_id, a.employee_id, a.started_at, a.finished_at,
                   q.pass_score, q.time_limit_min
            FROM quiz_attempts a
            JOIN quizzes q ON q.id = a.quiz_id
            WHERE a.id = %s
            LIMIT 1
            """,
            (attempt_id,),
        )
        attempt = cur.fetchone()
        if not attempt:
            return jsonify({"ok": False, "error": "attempt_not_found"}), 404

        if attempt.get("finished_at"):
            # Đã nộp rồi → trả lại kết quả hiện có
            return jsonify({
                "ok": True,
                "score": float(attempt.get("score") or 0),
                "passed": bool(attempt.get("passed") or False),
                "already_submitted": True,
            }), 200

        quiz_id = attempt["quiz_id"]
        pass_score = _row_to_float(attempt.get("pass_score"))
        time_limit_min = attempt.get("time_limit_min")

        # 2) Lấy cấu trúc quiz để chấm
        struct = _get_quiz_structure(int(quiz_id), cur)

        # 3) Chấm điểm
        total_score, perq = _grade_attempt(struct, answers_in)

        # 4) Đậu/rớt
        passed_flag = True if pass_score is None else (total_score >= pass_score)

        # 5) Xoá câu trả lời cũ (nếu có) rồi ghi lại chi tiết
        cur.execute("DELETE FROM quiz_answers WHERE attempt_id = %s", (attempt_id,))
        for row in perq:
            cur.execute(
                """
                INSERT INTO quiz_answers (attempt_id, question_id, selected_option_ids, is_correct, earned_points)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    attempt_id,
                    row["question_id"],
                    json.dumps(row["selected_option_ids"], ensure_ascii=False),
                    1 if row["is_correct"] else 0,
                    row["earned_points"],
                ),
            )

        # 6) Cập nhật attempt
        cur.execute(
            """
            UPDATE quiz_attempts
            SET finished_at = NOW(),
                score = %s,
                passed = %s
            WHERE id = %s
            """,
            (total_score, 1 if passed_flag else 0, attempt_id),
        )

        conn.commit()
        return jsonify({"ok": True, "score": total_score, "passed": passed_flag}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()