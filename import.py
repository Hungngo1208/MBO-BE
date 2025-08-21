#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from collections import defaultdict
import sys, traceback
import database  # file database.py của bạn

LEVELS = [
    ("corporation", "corporation"),
    ("company", "company"),
    ("factory", "factory"),
    ("division", "division"),
    ("sub_division", "sub_division"),
    ("section", "section"),
    ("group_name", "group"),
]

def fetch_employees(cur):
    cur.execute("""
        SELECT id, corporation, company, factory, division, sub_division, section, group_name
        FROM employees2026
        ORDER BY id
    """)
    rows = cur.fetchall()
    clean = []
    for r in rows:
        row = {}
        for k in r:
            v = r[k]
            if isinstance(v, str):
                v = v.strip()
            row[k] = v if v else None
        clean.append(row)
    return clean

def fetch_existing_units(cur):
    cur.execute("SELECT id, name, type, parent_id, code FROM organization_units")
    res = cur.fetchall()
    cache = {(row["type"], row["name"], row["parent_id"]): row["id"] for row in res}
    parent_of = {}
    children_of = defaultdict(list)
    codes = {}
    for row in res:
        uid = row["id"]
        p = row["parent_id"]
        parent_of[uid] = p
        children_of[p].append(uid)
        codes[uid] = row["code"]
    return cache, parent_of, children_of, codes

def next_child_code(cur, parent_id, parent_code):
    # tìm số thứ tự con hiện có của parent
    cur.execute("SELECT code FROM organization_units WHERE parent_id=%s ORDER BY code", (parent_id,))
    rows = cur.fetchall()
    if not rows:
        return (parent_code or "") + "1"
    else:
        # lấy số cuối cùng và +1
        last_code = rows[-1]["code"]
        return str(int(last_code) + 1)

def insert_unit(cur, name, unit_type, parent_id, parent_code):
    code = next_child_code(cur, parent_id, parent_code)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        INSERT INTO organization_units
        (name, type, parent_id, code, employee_id, created_at, updated_at, employee_count)
        VALUES (%s,%s,%s,%s,NULL,%s,%s,0)
    """, (name, unit_type, parent_id, code, now, now))
    return cur.lastrowid, code

def rebuild():
    conn = database.get_connection()
    conn.autocommit = False
    cur = conn.cursor(dictionary=True)
    try:
        employees = fetch_employees(cur)
        print(f"[INFO] Loaded {len(employees)} employees")

        unit_cache, parent_of, children_of, codes = fetch_existing_units(cur)

        members = defaultdict(list)
        emp_leaf = {}
        created_units = 0

        for emp in employees:
            parent_id = None
            parent_code = None
            last_unit_id = None
            for col, unit_type in LEVELS:
                name = emp.get(col)
                if name:
                    key = (unit_type, name, parent_id)
                    if key in unit_cache:
                        uid = unit_cache[key]
                        parent_id = uid
                        parent_code = codes[uid]
                    else:
                        uid, code = insert_unit(cur, name, unit_type, parent_id, parent_code)
                        unit_cache[key] = uid
                        parent_of[uid] = parent_id
                        children_of[parent_id].append(uid)
                        codes[uid] = code
                        parent_id = uid
                        parent_code = code
                        created_units += 1
                    last_unit_id = parent_id
            if last_unit_id:
                emp_leaf[emp["id"]] = last_unit_id
                members[last_unit_id].append(emp["id"])

        print(f"[INFO] Units created this run: {created_units}")

        # update employees
        for emp_id, leaf_id in emp_leaf.items():
            cur.execute("UPDATE employees2026 SET organization_unit_id=%s WHERE id=%s", (leaf_id, emp_id))

        # counts
        counts = defaultdict(int)
        for uid, lst in members.items():
            counts[uid] = len(lst)

        depth = {}
        def get_depth(u):
            if u in depth: return depth[u]
            p = parent_of.get(u)
            depth[u] = 0 if p is None else get_depth(p) + 1
            return depth[u]
        all_unit_ids = list(parent_of.keys())
        for u in all_unit_ids: get_depth(u)

        for u in sorted(all_unit_ids, key=lambda x: depth.get(x,0), reverse=True):
            for child in children_of.get(u, []):
                counts[u] += counts.get(child, 0)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for uid in all_unit_ids:
            cur.execute("UPDATE organization_units SET employee_count=%s, updated_at=%s WHERE id=%s",
                        (counts.get(uid,0), now, uid))

        for uid, lst in members.items():
            manager_id = lst[0] if len(lst) == 1 else None
            cur.execute("UPDATE organization_units SET employee_id=%s, updated_at=%s WHERE id=%s",
                        (manager_id, now, uid))

        conn.commit()
        print("[SUCCESS] Rebuild done with sequential codes")
    except Exception as e:
        conn.rollback()
        print("[ERROR]", e)
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    rebuild()
