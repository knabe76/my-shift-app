import streamlit as st
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# 接続URLは st.secrets["DATABASE_URL"] で読み込む（PostgreSQL/Supabase用。GitHubに上げてもパスワードが公開されない）
def _get_database_url() -> str:
    url = st.secrets["DATABASE_URL"]  # KeyError が発生した場合は起動時の try-except で捕捉して表示
    if isinstance(url, str) and url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url

_engine: Optional[Engine] = None

def get_engine() -> Engine:
    """SQLAlchemy エンジンを返す（st.secrets['DATABASE_URL'] から作成）。"""
    global _engine
    if _engine is None:
        _engine = create_engine(_get_database_url(), pool_pre_ping=True)
    return _engine

def get_conn():
    """PostgreSQL 接続を返す（psycopg2 互換。SQLAlchemy 経由で st.secrets['DATABASE_URL'] を使用）。"""
    return get_engine().raw_connection()

try:
    from streamlit_calendar import calendar as st_calendar
except ImportError:
    st_calendar = None

# 画面上の時間表示: 24時超は 24:00, 24:30, 25:00 ... 29:00 とする
def to_display_time(slot: str) -> str:
    """DBの 00:00〜05:00 を 24:00〜29:00 に変換（既に24以上ならそのまま）"""
    if not slot or ":" not in slot:
        return slot
    h, m = slot.split(":", 1)
    try:
        h_int = int(h)
        if 0 <= h_int <= 5:
            return f"{24 + h_int:02d}:{m}"
    except ValueError:
        pass
    return slot

# 17:00〜29:00 を30分刻みで生成（表示用ラベル）
def get_time_options():
    options = []
    for h in range(17, 24):
        options.append(f"{h:02d}:00")
        options.append(f"{h:02d}:30")
    for h in range(24, 30):
        options.append(f"{h:02d}:00")
        if h < 29:
            options.append(f"{h:02d}:30")
    return options

# PostgreSQL用テーブル作成（Supabase/Streamlit Cloud 対応）
# employees, availability, demand, demand_templates が存在しない場合に自動作成する
def init_db():
    conn = get_conn()
    try:
        c = conn.cursor()
        # employeesテーブル
        c.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                is_key_person SMALLINT DEFAULT 0,
                is_newbie SMALLINT DEFAULT 0
            );
        """)
        # is_newbieカラムがなければ追加（旧バージョン対応, 何度実行してもOK）
        c.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'employees' AND column_name = 'is_newbie'
        """)
        if c.fetchone() is None:
            c.execute("ALTER TABLE employees ADD COLUMN is_newbie SMALLINT DEFAULT 0;")
        # availabilityテーブル
        c.execute("""
            CREATE TABLE IF NOT EXISTS availability (
                id SERIAL PRIMARY KEY,
                employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
                date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                UNIQUE(employee_id, date)
            );
        """)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_avail_emp_date ON availability(employee_id, date);")
        # demandテーブル
        c.execute("""
            CREATE TABLE IF NOT EXISTS demand (
                date TEXT NOT NULL,
                slot TEXT NOT NULL,
                min_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,
                max_count INTEGER NOT NULL,
                PRIMARY KEY (date, slot)
            );
        """)
        # demand_templatesテーブル
        c.execute("""
            CREATE TABLE IF NOT EXISTS demand_templates (
                weekday SMALLINT NOT NULL,
                slot TEXT NOT NULL,
                min_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,
                max_count INTEGER NOT NULL,
                PRIMARY KEY (weekday, slot)
            );
        """)
        conn.commit()
    finally:
        conn.close()

# スタッフ登録（PostgreSQL: ON CONFLICTではなく単純INSERT。ユニーク制約は設けないが必要に応じて処理拡張可）
def add_employee(name: str, is_key_person: bool, is_newbie: bool = False):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO employees (name, is_key_person, is_newbie) VALUES (%s, %s, %s);",
            (name.strip(), 1 if is_key_person else 0, 1 if is_newbie else 0)
        )
        conn.commit()
    finally:
        conn.close()

# スタッフ一覧取得
def get_employees():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("SELECT id, name, is_key_person, COALESCE(is_newbie, 0) FROM employees ORDER BY id;")
        return c.fetchall()
    finally:
        conn.close()

# 希望シフト登録
def add_availability(employee_id: int, date: str, start_time: str, end_time: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        # availability (employee_id, date) はユニーク → UPSERT構文を使う
        c.execute(
            """
            INSERT INTO availability (employee_id, date, start_time, end_time)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(employee_id, date) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time;
            """,
            (employee_id, date, start_time, end_time)
        )
        conn.commit()
    finally:
        conn.close()

# 希望シフト一覧取得
def get_availabilities():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT a.id, e.name, a.date, a.start_time, a.end_time
            FROM availability a
            JOIN employees e ON a.employee_id = e.id
            ORDER BY a.date, a.start_time;
        """)
        return c.fetchall()
    finally:
        conn.close()

def get_availabilities_with_attributes():
    """(name, date, start_time, end_time, is_key_person, is_newbie) のリスト。start/end は表示形式。"""
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT e.name, a.date, a.start_time, a.end_time, COALESCE(e.is_key_person, 0), COALESCE(e.is_newbie, 0)
            FROM availability a
            JOIN employees e ON a.employee_id = e.id
            ORDER BY a.date, a.start_time
        """)
        rows = c.fetchall()
        return [(r[0], r[1], to_display_time(r[2]), to_display_time(r[3]), int(r[4]), int(r[5])) for r in rows]
    finally:
        conn.close()

def display_time_to_iso(date_str: str, time_display: str) -> str:
    """例: 2026-01-31, "29:00" → 2026-02-01T05:00:00"""
    if not time_display or ":" not in time_display:
        return date_str + "T00:00:00"
    h, m = time_display.split(":", 1)
    h_int, m_int = int(h), int(m) if m else 0
    d = datetime.strptime(date_str, "%Y-%m-%d")
    if h_int >= 24:
        d += timedelta(days=1)
        h_int -= 24
    return d.strftime("%Y-%m-%d") + f"T{h_int:02d}:{m_int:02d}:00"

def build_calendar_events_for_lib(avail_list):
    EVENT_COLORS = {(1, 1): "#9B59B6", (1, 0): "#3498DB", (0, 1): "#2ECC71", (0, 0): "#95A5A6"}
    events = []
    for idx, (name, date_str, start_d, end_d, is_kp, is_nb) in enumerate(avail_list):
        start_iso = display_time_to_iso(date_str, start_d)
        end_iso = display_time_to_iso(date_str, end_d)
        color = EVENT_COLORS.get((is_kp, is_nb), "#95A5A6")
        events.append({
            "id": str(idx),
            "title": f"{name}：{start_d}～{end_d}",
            "start": start_iso,
            "end": end_iso,
            "allDay": False,
            "backgroundColor": color,
            "borderColor": color,
        })
    return events

def get_availabilities_for_date(date_str: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT e.name, a.start_time, a.end_time
            FROM availability a
            JOIN employees e ON a.employee_id = e.id
            WHERE a.date = %s
            ORDER BY a.start_time, e.name;
        """, (date_str,))
        rows = c.fetchall()
        return [(r[0], to_display_time(r[1]), to_display_time(r[2])) for r in rows]
    finally:
        conn.close()

def get_availabilities_for_date_by_employee(date_str: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT employee_id, start_time, end_time
            FROM availability
            WHERE date = %s
            ORDER BY start_time;
        """, (date_str,))
        rows = c.fetchall()
        out = {}
        for emp_id, start, end in rows:
            if emp_id not in out:
                out[emp_id] = (to_display_time(start), to_display_time(end))
        return out
    finally:
        conn.close()

# 指定日の希望を一括保存（1人・1日・1レコードを徹底。既存は上書き, 休みの人は該当レコード削除）
def save_availabilities_for_date(date_str: str, items: list):
    """
    items = [(employee_id, start_time, end_time), ...]。
    UPSERT (ON CONFLICT) で同じ日・同じ人のデータは上書き。
    休みの人は該当レコード削除。
    """
    conn = get_conn()
    try:
        c = conn.cursor()
        emp_ids_in_items = [eid for eid, s, e in items if s and e and str(s).strip() and str(e).strip()]
        if emp_ids_in_items:
            placeholders = ",".join("%s" for _ in emp_ids_in_items)
            c.execute(
                f"DELETE FROM availability WHERE date = %s AND employee_id NOT IN ({placeholders})",
                (date_str, *emp_ids_in_items)
            )
        else:
            c.execute("DELETE FROM availability WHERE date = %s", (date_str,))
        for emp_id, start, end in items:
            if start and end and str(start).strip() and str(end).strip():
                start_s, end_s = start.strip(), end.strip()
                c.execute(
                    """
                    INSERT INTO availability (employee_id, date, start_time, end_time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(employee_id, date) DO UPDATE SET
                        start_time = excluded.start_time,
                        end_time = excluded.end_time;
                    """,
                    (emp_id, date_str, start_s, end_s)
                )
        conn.commit()
    finally:
        conn.close()

def cleanup_availability_data():
    """同一 (employee_id, date) の重複を1件だけ残し、終了≦開始の不正レコードを削除する（PostgreSQL方式）"""
    conn = get_conn()
    try:
        c = conn.cursor()
        # 重複削除: 一意制約あれば起こらないが念のため
        c.execute("""
            DELETE FROM availability a USING availability b
            WHERE a.employee_id = b.employee_id AND a.date = b.date AND a.id < b.id;
        """)
        dup_deleted = c.rowcount
        # 開始≧終了の不正データ削除（スロット番号化で比較）
        c.execute("SELECT id, start_time, end_time FROM availability;")
        rows = c.fetchall()
        invalid_ids = []
        for row in rows:
            sid, start_t, end_t = row[0], row[1], row[2]
            try:
                start_s = slot_str_to_index(to_display_time(start_t))
                end_s = slot_str_to_index(to_display_time(end_t))
                if end_s <= start_s:
                    invalid_ids.append(sid)
            except (ValueError, TypeError):
                invalid_ids.append(sid)
        if invalid_ids:
            placeholders = ",".join("%s" for _ in invalid_ids)
            c.execute(f"DELETE FROM availability WHERE id IN ({placeholders})", invalid_ids)
        invalid_deleted = len(invalid_ids)
        conn.commit()
        return dup_deleted, invalid_deleted
    finally:
        conn.close()

def _avail_copy_prev_callback():
    from datetime import timedelta
    avail_edit_date = st.session_state.get("avail_edit_date", datetime.now().date())
    avail_date_str = avail_edit_date.strftime("%Y-%m-%d")
    prev_date = (avail_edit_date - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_data = get_availabilities_for_date_by_employee(prev_date)
    st.session_state.avail_pending = ("copy_prev", avail_date_str, None)
    st.session_state.avail_pending_prev_data = prev_data
    # コールバック実行時点の「編集する日付」を退避し、rerun 後に復元する（日付が 2026-01-30 に戻る問題を防ぐ）
    st.session_state.avail_edit_date_preserve = avail_edit_date
    st.session_state.avail_need_rerun = True

def _avail_full_callback(date_str: str, emp_id: int):
    st.session_state.avail_pending = ("full", date_str, emp_id)
    st.session_state.avail_need_rerun = True

def _avail_off_callback(date_str: str, emp_id: int):
    st.session_state.avail_pending = ("off", date_str, emp_id)
    st.session_state.avail_need_rerun = True

def get_demand_for_date(date_str: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT slot, min_count, target_count, max_count FROM demand WHERE date = %s ORDER BY slot;",
            (date_str,)
        )
        rows = c.fetchall()
        return {to_display_time(row[0]): (row[1], row[2], row[3]) for row in rows}
    finally:
        conn.close()

def get_demand_template_for_weekday(weekday: int):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT slot, min_count, target_count, max_count FROM demand_templates WHERE weekday = %s ORDER BY slot;",
            (weekday,)
        )
        rows = c.fetchall()
        return {to_display_time(row[0]): (row[1], row[2], row[3]) for row in rows}
    finally:
        conn.close()

def save_demand_template_slot(weekday: int, slot: str, min_count: int, target_count: int, max_count: int):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO demand_templates (weekday, slot, min_count, target_count, max_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(weekday, slot) DO UPDATE SET
                min_count=excluded.min_count,
                target_count=excluded.target_count,
                max_count=excluded.max_count;
            """,
            (weekday, slot, min_count, target_count, max_count)
        )
        conn.commit()
    finally:
        conn.close()

def get_effective_demand_for_date(date_str: str, time_options: list = None):
    if time_options is None:
        time_options = get_time_options()
    default_min, default_tgt, default_max = 2, 3, 4
    date_demand = get_demand_for_date(date_str)
    weekday = datetime.strptime(date_str, "%Y-%m-%d").weekday()
    template_demand = get_demand_template_for_weekday(weekday)
    effective = {}
    for slot in time_options:
        if slot in date_demand:
            effective[slot] = date_demand[slot]
        elif slot in template_demand:
            effective[slot] = template_demand[slot]
        else:
            effective[slot] = (default_min, default_tgt, default_max)
    if date_demand:
        source = "override"
    elif template_demand:
        source = "template"
    else:
        source = "default"
    return effective, source

def save_demand_slot(date_str: str, slot: str, min_count: int, target_count: int, max_count: int):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO demand (date, slot, min_count, target_count, max_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(date, slot) DO UPDATE SET
                min_count=excluded.min_count,
                target_count=excluded.target_count,
                max_count=excluded.max_count;
            """,
            (date_str, slot, min_count, target_count, max_count)
        )
        conn.commit()
    finally:
        conn.close()

def apply_default_demand(date_str: str, min_c: int = 2, target_c: int = 3, max_c: int = 4):
    for slot in TIME_OPTIONS:
        save_demand_slot(date_str, slot, min_c, target_c, max_c)

# ---------- シフト自動生成（OR-Tools CP-SAT） ----------
def slot_str_to_index(slot_str: str) -> int:
    """表示形式 "17:00"〜"29:00" をスロット番号 0〜24 に変換"""
    h, m = slot_str.split(":", 1)
    h_int, m_int = int(h), int(m) if m else 0
    if 17 <= h_int <= 23:
        return (h_int - 17) * 2 + (1 if m_int >= 30 else 0)
    if 24 <= h_int <= 29:
        return 14 + (h_int - 24) * 2 + (1 if m_int >= 30 else 0)
    return 0

def get_availability_matrix_and_staff(date_str: str):
    all_staff = get_employees()
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT employee_id, start_time, end_time FROM availability WHERE date = %s;",
            (date_str,)
        )
        avails = c.fetchall()
    finally:
        conn.close()

    n_slots = 25
    staff_list = []
    avail_matrix = []

    for emp_id, name, is_key_person, is_newbie in all_staff:
        slot_ok = [False] * n_slots
        for row in avails:
            eid, start_time, end_time = row[0], row[1], row[2]
            if eid != emp_id:
                continue
            start_s = slot_str_to_index(to_display_time(start_time))
            end_s = slot_str_to_index(to_display_time(end_time))
            for s in range(start_s, min(end_s, n_slots)):
                slot_ok[s] = True
        staff_list.append((emp_id, name, is_key_person, is_newbie))
        avail_matrix.append(slot_ok)

    return staff_list, avail_matrix

def get_demand_arrays(date_str: str):
    """指定日の min_count, target_count, max_count を長さ25のリストで返す。曜日テンプレート／日付上書きを反映。"""
    demand_dict, _ = get_effective_demand_for_date(date_str, TIME_OPTIONS)
    min_c = [0] * 25
    target_c = [0] * 25
    max_c = [4] * 25
    for idx, slot in enumerate(TIME_OPTIONS):
        if slot in demand_dict:
            a, b, c = demand_dict[slot]
            min_c[idx], target_c[idx], max_c[idx] = a, b, c
    return min_c, target_c, max_c


def solve_shift(date_str: str, min_work_hours: float = 3.0, newbie_max_per_slot: int = 2):
    """
    指定日のシフトをCP-SATで解く。
    戻り値: (success, assign_matrix or None, staff_list, error_message)
    assign_matrix[i][s] = 1 ならスタッフiがスロットsに入る。
    """
    from ortools.sat.python import cp_model

    staff_list, avail_matrix = get_availability_matrix_and_staff(date_str)
    min_count, target_count, max_count = get_demand_arrays(date_str)
    n_staff = len(staff_list)
    n_slots = 25
    MIN_SLOTS = max(6, int(min_work_hours * 2))

    if n_staff == 0:
        return False, None, [], "スタッフが登録されていません。"
    is_key = [s[2] for s in staff_list]
    is_newbie = [s[3] for s in staff_list]

    model = cp_model.CpModel()
    assign = []
    for i in range(n_staff):
        row = []
        for s in range(n_slots):
            if avail_matrix[i][s]:
                row.append(model.NewBoolVar(f"assign_{i}_{s}"))
            else:
                row.append(None)
        assign.append(row)
    work = [model.NewBoolVar(f"work_{i}") for i in range(n_staff)]

    for i in range(n_staff):
        actives = [assign[i][s] for s in range(n_slots) if assign[i][s] is not None]
        if not actives:
            continue
        model.Add(sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None) >= 1).OnlyEnforceIf(work[i])
        model.Add(sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None) == 0).OnlyEnforceIf(work[i].Not())
    for i in range(n_staff):
        actives = [assign[i][s] for s in range(n_slots) if assign[i][s] is not None]
        if not actives:
            continue
        model.Add(sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None) >= MIN_SLOTS).OnlyEnforceIf(work[i])
        starts = []
        for s in range(n_slots):
            if assign[i][s] is None:
                continue
            prev_off = 1 if s == 0 else (1 - assign[i][s - 1]) if assign[i][s - 1] is not None else 1
            start_var = model.NewBoolVar(f"start_{i}_{s}")
            model.Add(assign[i][s] >= start_var)
            model.Add(prev_off >= start_var)
            model.Add(start_var >= assign[i][s] - (0 if s == 0 else (assign[i][s - 1] if assign[i][s - 1] is not None else 0)))
            starts.append(start_var)
        model.Add(sum(starts) <= 1)

    for s in range(n_slots):
        vars_s = [assign[i][s] for i in range(n_staff) if assign[i][s] is not None]
        if vars_s:
            model.Add(sum(vars_s) >= min_count[s])
            model.Add(sum(vars_s) <= max_count[s])
    for s in range(n_slots):
        key_in_slot = [assign[i][s] for i in range(n_staff) if is_key[i] and assign[i][s] is not None]
        if key_in_slot:
            model.Add(sum(key_in_slot) >= 1)
    for s in range(n_slots):
        newbie_in_slot = [assign[i][s] for i in range(n_staff) if is_newbie[i] and assign[i][s] is not None]
        if newbie_in_slot:
            model.Add(sum(newbie_in_slot) <= newbie_max_per_slot)

    abs_devs = []
    for s in range(n_slots):
        vars_s = [assign[i][s] for i in range(n_staff) if assign[i][s] is not None]
        if not vars_s:
            continue
        total_s = sum(vars_s)
        dev = model.NewIntVar(-25, 25, f"dev_{s}")
        model.Add(dev == total_s - target_count[s])
        abs_dev = model.NewIntVar(0, 25, f"abs_dev_{s}")
        model.AddAbsEquality(abs_dev, dev)
        abs_devs.append(abs_dev)
    slot_totals = [
        sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None)
        for i in range(n_staff)
    ]
    max_slots = model.NewIntVar(0, n_slots, "max_slots")
    min_slots_var = model.NewIntVar(0, n_slots, "min_slots")
    for i in range(n_staff):
        model.Add(max_slots >= slot_totals[i])
        model.Add(min_slots_var <= slot_totals[i])
    fairness = model.NewIntVar(0, n_slots, "fairness")
    model.Add(fairness == max_slots - min_slots_var)
    if abs_devs:
        model.Minimize(sum(abs_devs) * 10 + fairness)
    else:
        model.Minimize(fairness)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30.0
    status = solver.Solve(model)
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        result = []
        for i in range(n_staff):
            row = []
            for s in range(n_slots):
                row.append(int(solver.Value(assign[i][s])) if assign[i][s] is not None else 0)
            result.append(row)
        return True, result, staff_list, None
    reason = "解が見つかりませんでした。"
    if status == cp_model.INFEASIBLE:
        reason = "制約を満たす解が存在しません。キーマン不足・希望可能枠不足・最小人数の設定などを確認してください。"
    elif status == cp_model.UNKNOWN:
        reason = "時間制限で打ち切られたか、解の探索に失敗しました。"
    return False, None, staff_list, reason


def diagnose_shift_failure(date_str: str):
    """シフト生成が失敗した原因をスロット単位で診断する。"""
    staff_list, avail_matrix = get_availability_matrix_and_staff(date_str)
    min_count, _, _ = get_demand_arrays(date_str)
    n_staff = len(staff_list)
    n_slots = 25
    is_key = [s[2] for s in staff_list]
    is_newbie = [s[3] for s in staff_list]
    issues = []
    for s in range(n_slots):
        time_label = TIME_OPTIONS[s] + "〜" + (TIME_OPTIONS[s + 1] if s < n_slots - 1 else "閉店")
        total_avail = sum(1 for i in range(n_staff) if avail_matrix[i][s])
        key_avail = sum(1 for i in range(n_staff) if avail_matrix[i][s] and is_key[i])
        newbie_avail = sum(1 for i in range(n_staff) if avail_matrix[i][s] and is_newbie[i])
        min_req = min_count[s]
        if min_req == 0:
            continue
        if total_avail < min_req:
            issues.append((time_label, f"希望者が足りません（必要{min_req}に対し{total_avail}名）"))
            continue
        if key_avail == 0:
            issues.append((time_label, "キーマンが不在です"))
            continue
        newbie_max = 2
        max_assignable = total_avail - newbie_avail + min(newbie_avail, newbie_max)
        if max_assignable < min_req:
            issues.append((time_label, f"新人制限を考慮すると最小人数を満たせません（最大{max_assignable}名まで）"))
    return issues


SHIFT_COLOR_MAP = {
    "キーマン": "#1f77b4",
    "新人": "#2ca02c",
    "キーマン・新人": "#9467bd",
    "一般": "#7f7f7f",
}


def _assign_matrix_to_bars(assign_matrix, staff_list, base_dt, slot_minutes, n_slots):
    bars = []
    for i in range(len(staff_list)):
        emp_id, name, is_key_person, is_newbie = staff_list[i]
        row = assign_matrix[i]
        if is_key_person and is_newbie:
            type_label, color = "キーマン・新人", SHIFT_COLOR_MAP["キーマン・新人"]
        elif is_key_person:
            type_label, color = "キーマン", SHIFT_COLOR_MAP["キーマン"]
        elif is_newbie:
            type_label, color = "新人", SHIFT_COLOR_MAP["新人"]
        else:
            type_label, color = "一般", SHIFT_COLOR_MAP["一般"]
        s = 0
        while s < n_slots:
            if row[s] != 1:
                s += 1
                continue
            start_s = s
            while s < n_slots and row[s] == 1:
                s += 1
            start_dt = base_dt + timedelta(minutes=slot_minutes * start_s)
            end_dt = base_dt + timedelta(minutes=slot_minutes * s)
            bars.append((name, start_dt, end_dt, color))
    return bars


def build_availability_calendar_figure(avail_list):
    """希望シフト一覧を Plotly タイムラインで表示（streamlit-calendar のフォールバック用）。"""
    if not avail_list:
        return None
    import plotly.express as px
    from datetime import datetime as dt
    EVENT_COLORS = {(1, 1): "#9B59B6", (1, 0): "#3498DB", (0, 1): "#2ECC71", (0, 0): "#95A5A6"}
    rows = []
    for i, (name, date_str, start_d, end_d, is_kp, is_nb) in enumerate(avail_list):
        start_iso = display_time_to_iso(date_str, start_d)
        end_iso = display_time_to_iso(date_str, end_d)
        try:
            start_dt = dt.fromisoformat(start_iso.replace("Z", "+00:00")[:19])
            end_dt = dt.fromisoformat(end_iso.replace("Z", "+00:00")[:19])
        except Exception:
            continue
        color = EVENT_COLORS.get((is_kp, is_nb), "#95A5A6")
        rows.append({"Task": f"{name}：{start_d}～{end_d}", "Start": start_dt, "Finish": end_dt, "color": color})
    if not rows:
        return None
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task")
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=max(400, len(rows) * 32), margin=dict(l=180), showlegend=False)
    for i, row in enumerate(rows):
        if i < len(fig.data):
            fig.data[i].marker.color = row["color"]
    return fig


def build_gantt_figure(assign_matrix, staff_list, time_options):
    """1日分のガントチャート（週間表と同様のデザイン・色）。"""
    import plotly.express as px
    from datetime import datetime as dt
    base = dt(2000, 1, 1, 17, 0)
    slot_minutes = 30
    n_slots = len(time_options)
    bars = _assign_matrix_to_bars(assign_matrix, staff_list, base, slot_minutes, n_slots)
    if not bars:
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.update_layout(title="シフト表（割当なし）", height=300)
        return fig
    rows = [{"Task": name, "Start": s, "Finish": e, "Type": "キーマン" if c == SHIFT_COLOR_MAP["キーマン"] else "新人" if c == SHIFT_COLOR_MAP["新人"] else "キーマン・新人" if c == SHIFT_COLOR_MAP["キーマン・新人"] else "一般"} for name, s, e, c in bars]
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", color="Type", color_discrete_map=SHIFT_COLOR_MAP)
    fig.update_yaxes(autorange="reversed")
    fig.update_traces(marker=dict(line=dict(width=0)), width=0.8)
    base_end = dt(2000, 1, 2, 5, 0)
    hour_tickvals = [base + timedelta(hours=i) for i in range(13)]
    hour_ticktext = ["17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00", "24:00", "25:00", "26:00", "27:00", "28:00", "29:00"]
    fig.update_xaxes(range=[base, base_end], tickvals=hour_tickvals, ticktext=hour_ticktext, showgrid=True, gridwidth=1.2, gridcolor="rgba(0,0,0,0.4)")
    for h in range(13):
        fig.add_vline(x=hour_tickvals[h], line_width=0.8, line_dash="solid", line_color="rgba(0,0,0,0.35)")
    fig.update_layout(height=max(350, len(staff_list) * 32), margin=dict(l=120), legend_title="属性", plot_bgcolor="white")
    return fig


def build_weekly_shift_figure(gen_results, week_dates, time_options):
    """週間シフト表を1枚の Plotly 図で返す。"""
    import plotly.express as px
    from datetime import datetime as dt
    WEEKDAY_NAMES = ("月", "火", "水", "木", "金", "土", "日")
    base = dt(2000, 1, 1, 17, 0)
    base_end = dt(2000, 1, 2, 5, 0)
    slot_minutes = 30
    n_slots = len(time_options)
    rows = []
    y_order = []
    for date_str in week_dates:
        d = dt.strptime(date_str, "%Y-%m-%d")
        day_num = d.day
        wd = WEEKDAY_NAMES[d.weekday()]
        date_label = f"{day_num} {wd}"
        success, assign_matrix, staff_list, _, _ = gen_results.get(date_str, (False, None, [], None, None))
        if not success or assign_matrix is None or not staff_list:
            task_label = f"{date_label} ｜ （未生成）"
            rows.append({"Task": task_label, "Start": base, "Finish": base + timedelta(minutes=1), "Type": "一般"})
            y_order.append(task_label)
        else:
            day_bars = _assign_matrix_to_bars(assign_matrix, staff_list, base, slot_minutes, n_slots)
            if not day_bars:
                task_label = f"{date_label} ｜ （割当なし）"
                rows.append({"Task": task_label, "Start": base, "Finish": base + timedelta(minutes=1), "Type": "一般"})
                y_order.append(task_label)
            else:
                for name, start_dt, end_dt, color in day_bars:
                    task_label = f"{date_label} ｜ {name}"
                    type_name = "キーマン" if color == SHIFT_COLOR_MAP["キーマン"] else "新人" if color == SHIFT_COLOR_MAP["新人"] else "キーマン・新人" if color == SHIFT_COLOR_MAP["キーマン・新人"] else "一般"
                    rows.append({"Task": task_label, "Start": start_dt, "Finish": end_dt, "Type": type_name})
                    y_order.append(task_label)
    if not rows:
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.update_layout(title="週間シフト表（データなし）", height=400)
        return fig
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", color="Type", color_discrete_map=SHIFT_COLOR_MAP)
    fig.update_yaxes(autorange="reversed", categoryorder="array", categoryarray=list(dict.fromkeys(y_order)))
    fig.update_traces(marker=dict(line=dict(width=0)), width=0.8)
    hour_tickvals = [base + timedelta(hours=i) for i in range(13)]
    hour_ticktext = ["17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00", "24:00", "25:00", "26:00", "27:00", "28:00", "29:00"]
    fig.update_xaxes(range=[base, base_end], tickvals=hour_tickvals, ticktext=hour_ticktext, showgrid=True, gridwidth=1.2, gridcolor="rgba(0,0,0,0.4)", zeroline=False)
    fig.update_layout(title_text="週間シフト表", height=min(900, max(450, len(y_order) * 28)), margin=dict(l=160, r=50, t=50, b=55), legend_title="属性", plot_bgcolor="white")
    for h in range(13):
        fig.add_vline(x=hour_tickvals[h], line_width=0.8, line_dash="solid", line_color="rgba(0,0,0,0.35)")
    return fig


# ---------- 起動時: テーブル自動作成・クリーニング・エラーハンドリング ----------
TIME_OPTIONS = get_time_options()

try:
    init_db()
    if st.session_state.get("_avail_cleanup_done") is not True:
        dup_d, inv_d = cleanup_availability_data()
        if dup_d or inv_d:
            st.toast(f"希望シフトデータを整理しました（重複 {dup_d} 件・不正 {inv_d} 件削除）", icon="🧹")
        st.session_state._avail_cleanup_done = True
except KeyError:
    st.error("**DATABASE_URL が設定されていません。**")
    st.markdown(
        "Streamlit Cloud の場合は **Settings → Secrets** に、ローカルの場合は `.streamlit/secrets.toml` に、"
        "次の形式で `DATABASE_URL` を追加してください。"
    )
    st.code('DATABASE_URL = "postgresql://user:password@host:5432/dbname"', language="toml")
    st.stop()
except FileNotFoundError:
    st.error("**Secrets ファイルが見つかりません。**")
    st.markdown("`.streamlit/secrets.toml` を用意するか、Streamlit Cloud の Secrets で `DATABASE_URL` を設定してください。")
    st.stop()
except Exception as e:
    err_type = type(e).__name__
    err_msg = str(e).lower()
    st.error(f"**データベース接続に失敗しました**（{err_type}）")
    if "password" in err_msg or "authentication" in err_msg or "pg_auth" in err_msg:
        st.warning("原因の可能性: **パスワードまたはユーザー名の誤り**。Supabase の接続文字列を再確認してください。")
    elif "connection" in err_msg or "refused" in err_msg or "could not connect" in err_msg:
        st.warning("原因の可能性: **ホスト名・ポートの誤り、またはネットワーク／ファイアウォール**。接続先が正しいか確認してください。")
    elif "does not exist" in err_msg or "database" in err_msg:
        st.warning("原因の可能性: **データベース名の誤り**。Supabase の Connection string を確認してください。")
    else:
        st.warning(f"詳細: {e}")
    st.code(str(e), language=None)
    st.stop()


# ---------- ページ設定・サイドバー ----------
st.set_page_config(page_title="シフト管理", page_icon="📅", layout="wide")

if "min_work_hours" not in st.session_state:
    st.session_state.min_work_hours = 3.0
if "newbie_max_per_slot" not in st.session_state:
    st.session_state.newbie_max_per_slot = 2
if "default_min" not in st.session_state:
    st.session_state.default_min = 2
if "default_target" not in st.session_state:
    st.session_state.default_target = 3
if "default_max" not in st.session_state:
    st.session_state.default_max = 4

with st.sidebar:
    st.markdown("### ⚙️ 共通設定")
    st.markdown("シフト自動生成や一括適用で使う値です。")
    st.slider("最低勤務時間（時間）", min_value=1.0, max_value=6.0, step=0.5, key="min_work_hours", help="入る場合はこの時間以上連続で入る必要があります。")
    st.slider("同一スロットの新人上限", min_value=1, max_value=4, value=2, key="newbie_max_per_slot")
    st.slider("デフォルト最小人数", min_value=1, max_value=5, value=2, key="default_min")
    st.slider("デフォルト目標人数", min_value=1, max_value=5, value=3, key="default_target")
    st.slider("デフォルト最大人数", min_value=1, max_value=6, value=4, key="default_max")

st.title("📅 店長専用シフト管理システム")

tab1, tab2, tab3, tab4 = st.tabs(["👥 スタッフ管理", "📈 必要人数", "✍️ 希望シフト一覧", "🤖 シフト自動生成"])

# ---------- タブ1: スタッフ管理 ----------
with tab1:
    st.header("👥 スタッフ管理")
    with st.expander("➕ 新しいスタッフを追加する"):
        with st.form("staff_form"):
            name = st.text_input("スタッフ名")
            is_kp = st.checkbox("キーマン（責任者）")
            is_nb = st.checkbox("新人")
            if st.form_submit_button("登録"):
                if name and name.strip():
                    add_employee(name.strip(), is_kp, is_nb)
                    st.success(f"{name.strip()} さんを登録しました")
                    st.rerun()
    st.subheader("現在のスタッフ一覧")
    employees = get_employees()
    if employees:
        df_emp = pd.DataFrame(employees, columns=["ID", "名前", "キーマン", "新人"])
        st.dataframe(df_emp, use_container_width=True)
    else:
        st.info("スタッフがまだ登録されていません。")

# ---------- タブ2: 必要人数（曜日テンプレート + 日付別上書き） ----------
with tab2:
    st.header("📈 必要人数の設定")
    WEEKDAY_NAMES = ("月", "火", "水", "木", "金", "土", "日")
    template_weekday = st.selectbox("曜日別テンプレートを編集", range(7), format_func=lambda x: WEEKDAY_NAMES[x], key="demand_template_weekday")
    template_existing = get_demand_template_for_weekday(template_weekday)
    default_min, default_tgt, default_max = 2, 3, 4
    if template_existing:
        vals = list(template_existing.values())
        if vals:
            default_min = vals[0][0]
            default_tgt = vals[0][1]
            default_max = vals[0][2]
    with st.form("demand_template_form"):
        st.markdown(f"**{WEEKDAY_NAMES[template_weekday]}曜日**の全スロットに適用する 最小・目標・最大人数")
        tm = st.number_input("最小人数", min_value=0, max_value=5, value=default_min, key="tm_all")
        tt = st.number_input("目標人数", min_value=0, max_value=5, value=default_tgt, key="tt_all")
        tx = st.number_input("最大人数", min_value=0, max_value=6, value=default_max, key="tx_all")
        if st.form_submit_button("この曜日テンプレートを全スロットに保存"):
            for slot in TIME_OPTIONS:
                save_demand_template_slot(template_weekday, slot, tm, tt, tx)
            st.success("テンプレートを保存しました")
            st.rerun()
    st.divider()
    st.subheader("特定日付の上書き")
    if "demand_date" not in st.session_state:
        st.session_state.demand_date = datetime.now().date()
    demand_date = st.date_input("日付を選択", key="demand_date")
    demand_date_str = demand_date.strftime("%Y-%m-%d")
    effective, source = get_effective_demand_for_date(demand_date_str)
    st.caption(f"反映中: {'日付別上書き' if source == 'override' else '曜日テンプレート' if source == 'template' else 'デフォルト'}")
    min_c = st.session_state.default_min
    tgt_c = st.session_state.default_target
    max_c = st.session_state.default_max
    if st.button("この日の全スロットにデフォルト値を一括適用"):
        apply_default_demand(demand_date_str, min_c, tgt_c, max_c)
        st.success(f"{demand_date_str} に一括適用しました")
        st.rerun()

# ---------- タブ3: 希望シフト一覧（カレンダー + 日付別編集・フル/休み/前日コピー） ----------
with tab3:
    st.header("✍️ 希望シフト一覧")
    avail_list = get_availabilities_with_attributes()
    if avail_list:
        if st_calendar:
            events = build_calendar_events_for_lib(avail_list)
            st_calendar(events=events, options={"initialView": "dayGridMonth"})
        else:
            fig = build_availability_calendar_figure(avail_list)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(pd.DataFrame(avail_list, columns=["名前", "日付", "開始", "終了", "キーマン", "新人"]))
    st.divider()
    st.subheader("日付別に希望を編集")
    if "avail_edit_date" not in st.session_state:
        st.session_state.avail_edit_date = datetime.now().date()
    avail_edit_date = st.date_input("編集する日付", key="avail_edit_date")
    avail_date_str = avail_edit_date.strftime("%Y-%m-%d")
    if st.session_state.get("avail_edit_date_preserve") is not None:
        st.session_state.avail_edit_date = st.session_state.avail_edit_date_preserve
        st.session_state.avail_edit_date_preserve = None
    if st.session_state.get("avail_need_rerun"):
        st.session_state.avail_need_rerun = False
        st.rerun()
    pending = st.session_state.get("avail_pending")
    if pending:
        kind, pdate, emp_id = pending[0], pending[1], pending[2]
        if kind == "copy_prev":
            prev_data = st.session_state.get("avail_pending_prev_data", {})
            items = [(eid, start, end) for eid, (start, end) in prev_data.items()]
            save_availabilities_for_date(pdate, items)
            st.toast("前日の希望をコピーして保存しました")
        elif kind == "full" and emp_id is not None:
            save_availabilities_for_date(pdate, [(emp_id, "17:00", "29:00")])
            st.toast("フルで保存しました")
        elif kind == "off" and emp_id is not None:
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute("DELETE FROM availability WHERE date = %s AND employee_id = %s", (pdate, emp_id))
                conn.commit()
            finally:
                conn.close()
            st.toast("休みで反映しました")
        st.session_state.avail_pending = None
        st.rerun()
    emps = get_employees()
    current_avails = get_availabilities_for_date_by_employee(avail_date_str)
    st.write(f"**{avail_date_str}** の希望")
    cols_btn = st.columns(min(len(emps) + 1, 8))
    with cols_btn[0]:
        st.button("前日をコピー", key="copy_prev_btn", on_click=_avail_copy_prev_callback)
    for idx, (eid, ename, is_kp, is_nb) in enumerate(emps):
        with cols_btn[(idx % (len(cols_btn) - 1)) + 1]:
            st.button(f"フル\n{ename}"[:8], key=f"full_{eid}", on_click=_avail_full_callback, args=(avail_date_str, eid))
            st.button(f"休み\n{ename}"[:8], key=f"off_{eid}", on_click=_avail_off_callback, args=(avail_date_str, eid))
    new_items = []
    for eid, ename, is_kp, is_nb in emps:
        default_start, default_end = current_avails.get(eid, ("17:00", "29:00"))
        i_start = TIME_OPTIONS.index(default_start) if default_start in TIME_OPTIONS else 0
        i_end = TIME_OPTIONS.index(default_end) if default_end in TIME_OPTIONS else len(TIME_OPTIONS) - 1
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            st.write(f"**{ename}**")
        with c2:
            start_t = st.selectbox("開始", TIME_OPTIONS, index=i_start, key=f"s_{eid}")
        with c3:
            end_t = st.selectbox("終了", TIME_OPTIONS, index=i_end, key=f"e_{eid}")
        new_items.append((eid, start_t, end_t))
    if st.button("この日の全データを一括保存"):
        save_availabilities_for_date(avail_date_str, new_items)
        st.success(f"{avail_date_str} の希望を保存しました")
        st.rerun()

# ---------- タブ4: シフト自動生成（期間指定・進捗・週間シフト表・失敗詳細） ----------
with tab4:
    st.header("🤖 シフト自動生成")
    c_start, c_end = st.columns(2)
    with c_start:
        gen_start = st.date_input("開始日", value=datetime.now().date(), key="gen_start")
    with c_end:
        gen_end = st.date_input("終了日", value=datetime.now().date(), key="gen_end")
    if gen_end < gen_start:
        gen_end = gen_start
    if st.button("選択期間のシフトを自動生成"):
        gen_results = {}
        date_list = []
        d = gen_start
        while d <= gen_end:
            date_list.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        progress = st.progress(0.0)
        success_count = 0
        fail_count = 0
        for i, date_str in enumerate(date_list):
            progress.progress((i + 1) / len(date_list), text=date_str)
            ok, assign_matrix, staff_list, err_msg = solve_shift(
                date_str,
                min_work_hours=st.session_state.min_work_hours,
                newbie_max_per_slot=st.session_state.newbie_max_per_slot,
            )
            if ok:
                gen_results[date_str] = (True, assign_matrix, staff_list, None, None)
                success_count += 1
            else:
                gen_results[date_str] = (False, None, staff_list, err_msg, None)
                fail_count += 1
        progress.empty()
        st.success(f"完了: 成功 {success_count} 日、失敗 {fail_count} 日")
        st.session_state.gen_results = gen_results
        st.session_state.gen_date_list = date_list
    if st.session_state.get("gen_results"):
        gen_results = st.session_state.gen_results
        date_list = st.session_state.get("gen_date_list", [])
        st.subheader("週間シフト表")
        week_starts = sorted(set(datetime.strptime(d, "%Y-%m-%d").date().isocalendar()[1] for d in date_list))
        week_options = [f"{ws}週目" for ws in week_starts]
        selected_week = st.selectbox("表示する週", week_options, key="week_select")
        if selected_week:
            ws = int(selected_week.replace("週目", ""))
            week_dates = [d for d in date_list if datetime.strptime(d, "%Y-%m-%d").date().isocalendar()[1] == ws]
            week_dates.sort()
            if week_dates:
                fig_week = build_weekly_shift_figure(gen_results, week_dates, TIME_OPTIONS)
                st.plotly_chart(fig_week, use_container_width=True)
        st.subheader("1日分のシフト表")
        ok_dates = [d for d in date_list if gen_results.get(d, (False,))[0]]
        if ok_dates:
            pick_date = st.selectbox("日付を選択", ok_dates, key="pick_single_date")
            if pick_date:
                _, assign_matrix, staff_list, _, _ = gen_results[pick_date]
                if assign_matrix and staff_list:
                    fig_day = build_gantt_figure(assign_matrix, staff_list, TIME_OPTIONS)
                    st.plotly_chart(fig_day, use_container_width=True)
        failed_dates = [d for d, r in gen_results.items() if not r[0]]
        if failed_dates:
            with st.expander("失敗した日の詳細"):
                for date_str in failed_dates:
                    _, _, _, err_msg, _ = gen_results[date_str]
                    issues = diagnose_shift_failure(date_str)
                    st.markdown(f"**{date_str}** — {err_msg}")
                    if issues:
                        for time_label, reason in issues:
                            st.caption(f"{time_label}: {reason}")