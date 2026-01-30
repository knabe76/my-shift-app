import streamlit as st
import psycopg2
import time
import pandas as pd
from datetime import datetime, timedelta


def get_conn():
    """Streamlit Secrets から Supabase（PostgreSQL）接続情報を読み、接続を返す。"""
    s = st.secrets.get("supabase") or st.secrets.get("postgres")
    if not s:
        raise RuntimeError(
            "StreamlitのSecretsに supabase または postgres の接続情報を設定してください。"
            " .streamlit/secrets.toml に host, port, dbname, user, password または database_url を追加してください。"
        )
    url = s.get("database_url")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=s["host"],
        port=int(s.get("port", 5432)),
        dbname=s.get("dbname") or s.get("database", "postgres"),
        user=s["user"],
        password=s["password"],
    )

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

# データベース接続とテーブル作成（PostgreSQL / Supabase）
def init_db():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                is_key_person SMALLINT DEFAULT 0,
                is_newbie SMALLINT DEFAULT 0
            )
        """)
        c.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'employees' AND column_name = 'is_newbie'
        """)
        if c.fetchone() is None:
            c.execute("ALTER TABLE employees ADD COLUMN is_newbie SMALLINT DEFAULT 0")
        c.execute("""
            CREATE TABLE IF NOT EXISTS availability (
                id SERIAL PRIMARY KEY,
                employee_id INTEGER NOT NULL REFERENCES employees(id),
                date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                UNIQUE(employee_id, date)
            )
        """)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_avail_emp_date ON availability(employee_id, date)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS demand (
                date TEXT NOT NULL,
                slot TEXT NOT NULL,
                min_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,
                max_count INTEGER NOT NULL,
                PRIMARY KEY (date, slot)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS demand_templates (
                weekday SMALLINT NOT NULL,
                slot TEXT NOT NULL,
                min_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,
                max_count INTEGER NOT NULL,
                PRIMARY KEY (weekday, slot)
            )
        """)
        conn.commit()
    finally:
        conn.close()

# スタッフ登録
def add_employee(name: str, is_key_person: bool, is_newbie: bool = False):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO employees (name, is_key_person, is_newbie) VALUES (%s, %s, %s)",
            (name.strip(), 1 if is_key_person else 0, 1 if is_newbie else 0)
        )
        conn.commit()
    finally:
        conn.close()

# スタッフ一覧取得 (id, name, is_key_person, is_newbie)
def get_employees():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("SELECT id, name, is_key_person, COALESCE(is_newbie, 0) FROM employees ORDER BY id")
        return c.fetchall()
    finally:
        conn.close()

# 希望シフト登録
def add_availability(employee_id: int, date: str, start_time: str, end_time: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO availability (employee_id, date, start_time, end_time) VALUES (%s, %s, %s, %s)",
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
            ORDER BY a.date, a.start_time
        """)
        return c.fetchall()
    finally:
        conn.close()

# カレンダー用：希望シフト一覧（スタッフ属性付き）
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

# 表示時間 "17:00"〜"29:00" を ISO8601 に変換（24:00→翌日00:00, 25:00→翌日01:00, …, 29:00→翌日05:00）
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

# 希望シフト一覧を streamlit-calendar 用イベント形式に変換（allDay: false, title, start, end 必須）
def build_calendar_events_for_lib(avail_list):
    """avail_list = [(name, date_str, start_d, end_d, is_kp, is_nb), ...] → カレンダー用辞書のリスト"""
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

# 指定日の希望シフト一覧（スタッフ名・開始・終了）
def get_availabilities_for_date(date_str: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT e.name, a.start_time, a.end_time
            FROM availability a
            JOIN employees e ON a.employee_id = e.id
            WHERE a.date = %s
            ORDER BY a.start_time, e.name
        """, (date_str,))
        rows = c.fetchall()
        return [(r[0], to_display_time(r[1]), to_display_time(r[2])) for r in rows]
    finally:
        conn.close()

# 指定日の希望を employee_id → (start_time, end_time) で取得（1人1件、先着）
def get_availabilities_for_date_by_employee(date_str: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT employee_id, start_time, end_time
            FROM availability
            WHERE date = %s
            ORDER BY start_time
        """, (date_str,))
        rows = c.fetchall()
        out = {}
        for emp_id, start, end in rows:
            if emp_id not in out:
                out[emp_id] = (to_display_time(start), to_display_time(end))
        return out
    finally:
        conn.close()

# 指定日の希望を一括保存（1人・1日・1レコードを徹底。既存は上書き）
def save_availabilities_for_date(date_str: str, items: list):
    """items = [(employee_id, start_time, end_time), ...]。UPSERT で同じ日・同じ人のデータは上書き。休みの人は該当レコード削除。"""
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
                    """INSERT INTO availability (employee_id, date, start_time, end_time)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT(employee_id, date) DO UPDATE SET
                       start_time = excluded.start_time,
                       end_time = excluded.end_time""",
                    (emp_id, date_str, start_s, end_s)
                )
        conn.commit()
    finally:
        conn.close()

# 希望シフトの重複・不正データをクリーニング（起動時に1回実行）
def cleanup_availability_data():
    """同一 (employee_id, date) の重複を残り1件にし、終了≦開始の不正レコードを削除する。"""
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            DELETE FROM availability a USING availability b
            WHERE a.employee_id = b.employee_id AND a.date = b.date AND a.id < b.id
        """)
        dup_deleted = c.rowcount
        c.execute("SELECT id, start_time, end_time FROM availability")
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

# 希望シフト入力タブ用コールバック（session_state のみ更新。rerun は通常フロー側で実行）
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

# 指定日の必要人数取得（日付専用・上書き分のみ。スロット → (min, target, max)）
def get_demand_for_date(date_str: str):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT slot, min_count, target_count, max_count FROM demand WHERE date = %s ORDER BY slot",
            (date_str,)
        )
        rows = c.fetchall()
        return {to_display_time(row[0]): (row[1], row[2], row[3]) for row in rows}
    finally:
        conn.close()

# 曜日別テンプレート取得（weekday: 0=月〜6=日。スロット → (min, target, max)）
def get_demand_template_for_weekday(weekday: int):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT slot, min_count, target_count, max_count FROM demand_templates WHERE weekday = %s ORDER BY slot",
            (weekday,)
        )
        rows = c.fetchall()
        return {to_display_time(row[0]): (row[1], row[2], row[3]) for row in rows}
    finally:
        conn.close()

# 曜日別テンプレートを1スロット保存（UPSERT）
def save_demand_template_slot(weekday: int, slot: str, min_count: int, target_count: int, max_count: int):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            """INSERT INTO demand_templates (weekday, slot, min_count, target_count, max_count)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT(weekday, slot) DO UPDATE SET
               min_count=excluded.min_count,
               target_count=excluded.target_count,
               max_count=excluded.max_count""",
            (weekday, slot, min_count, target_count, max_count)
        )
        conn.commit()
    finally:
        conn.close()

# 指定日の「実効」必要人数を取得（日付上書き → 曜日テンプレート → 初期値2,3,4）。戻り値: (dict, source)
# source: "override"=日付別上書き, "template"=曜日デフォルト, "default"=初期値
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

# 必要人数を1スロット保存（日付別・上書き用 UPSERT）
def save_demand_slot(date_str: str, slot: str, min_count: int, target_count: int, max_count: int):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            """INSERT INTO demand (date, slot, min_count, target_count, max_count)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT(date, slot) DO UPDATE SET
               min_count=excluded.min_count,
               target_count=excluded.target_count,
               max_count=excluded.max_count""",
            (date_str, slot, min_count, target_count, max_count)
        )
        conn.commit()
    finally:
        conn.close()

# 全スロットにデフォルト値を一括適用（min_c, target_c, max_c はサイドバー設定を使用）
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
    """指定日のスタッフ一覧と、スタッフ×スロットの「入れる」行列を返す。staff_list は (emp_id, name, is_key_person, is_newbie)。"""
    all_staff = get_employees()
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT employee_id, start_time, end_time FROM availability WHERE date = %s",
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
    MIN_SLOTS = max(6, int(min_work_hours * 2))  # 30分刻み

    if n_staff == 0:
        return False, None, [], "スタッフが登録されていません。"
    is_key = [s[2] for s in staff_list]
    is_newbie = [s[3] for s in staff_list]

    model = cp_model.CpModel()

    # assign[i][s] = 1  iff スタッフ i がスロット s に入る
    assign = []
    for i in range(n_staff):
        row = []
        for s in range(n_slots):
            if avail_matrix[i][s]:
                row.append(model.NewBoolVar(f"assign_{i}_{s}"))
            else:
                row.append(None)
        assign.append(row)

    # work[i] = 1 iff スタッフ i はその日少なくとも1スロット入る
    work = [model.NewBoolVar(f"work_{i}") for i in range(n_staff)]

    # 希望優先: 入れるスロットだけ変数があるので、Noneのスロットは0扱い
    for i in range(n_staff):
        model.Add(sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None) >= 1).OnlyEnforceIf(work[i])
        model.Add(sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None) == 0).OnlyEnforceIf(work[i].Not())

    # 最低勤務・中抜き禁止: 連続6スロット以上・1ブロック
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
        ends = []
        for s in range(n_slots):
            if assign[i][s] is None:
                continue
            next_off = 1 if s == n_slots - 1 else (1 - assign[i][s + 1]) if assign[i][s + 1] is not None else 1
            end_var = model.NewBoolVar(f"end_{i}_{s}")
            model.Add(assign[i][s] >= end_var)
            model.Add(next_off >= end_var)
            model.Add(end_var >= assign[i][s] - (0 if s == n_slots - 1 else (assign[i][s + 1] if assign[i][s + 1] is not None else 0)))
            ends.append(end_var)
        model.Add(sum(ends) <= 1)

    # スロット別人数（最小・最大）
    for s in range(n_slots):
        vars_s = [assign[i][s] for i in range(n_staff) if assign[i][s] is not None]
        if vars_s:
            model.Add(sum(vars_s) >= min_count[s])
            model.Add(sum(vars_s) <= max_count[s])

    # キーマン: 各スロットに1名以上
    for s in range(n_slots):
        key_in_slot = []
        for i in range(n_staff):
            if is_key[i] and assign[i][s] is not None:
                key_in_slot.append(assign[i][s])
        if key_in_slot:
            model.Add(sum(key_in_slot) >= 1)

    # 新人制限: 同一スロットに新人（is_newbie）は最大 newbie_max_per_slot 名まで
    for s in range(n_slots):
        newbie_in_slot = []
        for i in range(n_staff):
            if is_newbie[i] and assign[i][s] is not None:
                newbie_in_slot.append(assign[i][s])
        if newbie_in_slot:
            model.Add(sum(newbie_in_slot) <= newbie_max_per_slot)

    # 目標人数への近づけ（偏差の絶対値の和を最小化）
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

    # 公平性: 各スタッフの総スロット数のばらつき（最大－最小を最小化）
    slot_totals = [
        sum(assign[i][s] for s in range(n_slots) if assign[i][s] is not None)
        for i in range(n_staff)
    ]
    max_slots = model.NewIntVar(0, n_slots, "max_slots")
    min_slots = model.NewIntVar(0, n_slots, "min_slots")
    for i in range(n_staff):
        model.Add(max_slots >= slot_totals[i])
        model.Add(min_slots <= slot_totals[i])
    fairness = model.NewIntVar(0, n_slots, "fairness")
    model.Add(fairness == max_slots - min_slots)

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
                if assign[i][s] is not None:
                    row.append(int(solver.Value(assign[i][s])))
                else:
                    row.append(0)
            result.append(row)
        return True, result, staff_list, None
    else:
        reason = "解が見つかりませんでした。"
        if status == cp_model.INFEASIBLE:
            reason = "制約を満たす解が存在しません。キーマン不足・希望可能枠不足・最小人数の設定などを確認してください。"
        elif status == cp_model.UNKNOWN:
            reason = "時間制限で打ち切られたか、解の探索に失敗しました。"
        return False, None, staff_list, reason

def diagnose_shift_failure(date_str: str):
    """
    シフト生成が失敗した原因をスロット単位で診断する。
    戻り値: [(時間帯ラベル, 原因メッセージ), ...]
    """
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
        newbie_max = 2  # 診断時はデフォルト
        max_assignable = total_avail - newbie_avail + min(newbie_avail, newbie_max)
        if max_assignable < min_req:
            issues.append((time_label, f"新人制限を考慮すると最小人数を満たせません（最大{max_assignable}名まで）"))

    return issues

def build_availability_calendar_figure(avail_list):
    """希望シフト一覧を Plotly のタイムライン（ガント風）で表示する Figure を返す。streamlit-calendar のフォールバック用。"""
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
        label = f"{name}：{start_d}～{end_d}"
        rows.append({"Task": label, "Start": start_dt, "Finish": end_dt, "color": color})
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

# 属性別バー色（週間シフト表・1日表で共通）
SHIFT_COLOR_MAP = {
    "キーマン": "#1f77b4",
    "新人": "#2ca02c",
    "キーマン・新人": "#9467bd",
    "一般": "#7f7f7f",
}

def _assign_matrix_to_bars(assign_matrix, staff_list, base_dt, slot_minutes, n_slots):
    """assign_matrix から (staff_label, start_dt, end_dt, color) のリストを返す。名前はバー左用に短く。"""
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

def build_weekly_shift_figure(gen_results, week_dates, time_options):
    """週間シフト表を1枚の Plotly 図で返す。week_dates = [月,火,...,日] の7日分。左に日付(曜日)｜スタッフ名、横軸17:00〜29:00、1時間刻みグリッド。"""
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
                    type_label = "キーマン" if color == SHIFT_COLOR_MAP["キーマン"] else "新人" if color == SHIFT_COLOR_MAP["新人"] else "キーマン・新人" if color == SHIFT_COLOR_MAP["キーマン・新人"] else "一般"
                    task_label = f"{date_label} ｜ {name}"
                    rows.append({"Task": task_label, "Start": start_dt, "Finish": end_dt, "Type": type_label})
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
    fig.update_xaxes(
        range=[base, base_end],
        tickvals=hour_tickvals,
        ticktext=hour_ticktext,
        showgrid=True,
        gridwidth=1.2,
        gridcolor="rgba(0,0,0,0.4)",
        zeroline=False,
    )
    fig.update_layout(
        title_text="週間シフト表",
        height=min(900, max(450, len(y_order) * 28)),
        margin=dict(l=160, r=50, t=50, b=55),
        legend_title="属性",
        plot_bgcolor="white",
        xaxis_title="",
        yaxis_title="",
    )
    for h in range(13):
        fig.add_vline(x=hour_tickvals[h], line_width=0.8, line_dash="solid", line_color="rgba(0,0,0,0.35)")
    return fig

def build_gantt_figure(assign_matrix, staff_list, time_options):
    """1日分のガントチャート（週間表と同様のデザイン・色）。"""
    import plotly.express as px
    from datetime import datetime as dt, timedelta

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

# 初期化（DB作成 → 希望シフトの重複・不正データを1回だけクリーニング）
init_db()
if st.session_state.get("_avail_cleanup_done") is not True:
    dup_d, inv_d = cleanup_availability_data()
    if dup_d or inv_d:
        st.toast(f"希望シフトデータを整理しました（重複 {dup_d} 件・不正 {inv_d} 件削除）", icon="🧹")
    st.session_state._avail_cleanup_done = True
TIME_OPTIONS = get_time_options()

# サイドバー: アプリ全体の共通設定
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
    if "min_work_hours" not in st.session_state:
        st.session_state.min_work_hours = 3.0
    st.slider(
        "最低勤務時間（時間）",
        min_value=1.0,
        max_value=6.0,
        step=0.5,
        key="min_work_hours",
        help="入る場合はこの時間以上連続で入る必要があります。",
    )
    if "newbie_max_per_slot" not in st.session_state:
        st.session_state.newbie_max_per_slot = 2
    st.slider(
        "同一スロットの新人上限（名）",
        min_value=1,
        max_value=5,
        key="newbie_max_per_slot",
        help="各時間帯に新人は最大この人数までです。",
    )
    st.markdown("---")
    st.markdown("**デフォルト必要人数（一括適用時）**")
    if "default_min" not in st.session_state:
        st.session_state.default_min = 2
    if "default_target" not in st.session_state:
        st.session_state.default_target = 3
    if "default_max" not in st.session_state:
        st.session_state.default_max = 4
    st.number_input("最小人数", min_value=0, max_value=10, key="default_min")
    st.number_input("目標人数", min_value=0, max_value=10, key="default_target")
    st.number_input("最大人数", min_value=0, max_value=10, key="default_max")

# カスタムスタイル（統一感のある色・余白）
st.markdown("""
<style>
    .stMetric { background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%); padding: 1rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
    div[data-testid="stMetricValue"] { color: #1e40af; }
    .stSuccess { border-left: 4px solid #059669; }
    .stError { border-left: 4px solid #dc2626; }
</style>
""", unsafe_allow_html=True)

# 起動時: streamlit-calendar 未インストールなら警告
if st_calendar is None:
    st.warning(
        "**カレンダー表示**には `streamlit-calendar` が必要です。"
        " インストールするにはターミナルで `pip install streamlit-calendar` を実行し、アプリを再読み込みしてください。"
    )

tab1, tab2, tab3, tab4 = st.tabs([
    "1. スタッフ管理",
    "2. 需要（必要人数）設定",
    "3. 希望シフト入力",
    "4. シフト自動生成",
])

# ---------- タブ1: スタッフ管理 ----------
with tab1:
    st.subheader("スタッフ登録")
    st.caption("スタッフ名とキーマン・新人の有無を入力し、登録ボタンで一覧に追加します。")
    with st.form("register_form", clear_on_submit=True):
        name = st.text_input("スタッフ名", placeholder="名前を入力してください")
        col_k, col_n = st.columns(2)
        with col_k:
            is_key_person = st.checkbox("キーマン", help="営業中は各スロットに1名以上必要です")
        with col_n:
            is_newbie = st.checkbox("新人", help="同一スロットに新人は最大2名までの制約に使います")
        submitted = st.form_submit_button("👤 登録")

        if submitted:
            if name and name.strip():
                add_employee(name, is_key_person, is_newbie)
                st.success(f"**{name.strip()}** さんをスタッフ一覧に追加しました。")
                st.rerun()
            else:
                st.warning("スタッフ名を入力してください。")

    st.subheader("登録スタッフ一覧")
    employees = get_employees()
    if employees:
        for emp_id, name, is_key_person, is_newbie in employees:
            labels = []
            if is_key_person:
                labels.append("✓ キーマン")
            if is_newbie:
                labels.append("新人")
            label_str = " ".join(labels) if labels else ""
            st.write(f"- **{name}** {label_str}")
    else:
        st.info("まだスタッフが登録されていません。")

# ---------- タブ2: 需要（必要人数）設定 ----------
WEEKDAY_NAMES = ("月", "火", "水", "木", "金", "土", "日")

with tab2:
    st.subheader("必要人数設定")
    st.caption("日付を選ぶと、その日の必要人数を編集できます。基本は曜日別テンプレート、特定日だけ上書きできます。")

    # 曜日別デフォルト設定を編集（エクスパンダー）
    with st.expander("📅 曜日別デフォルト設定を編集", expanded=False):
        st.caption("各曜日の基本パターンを設定します。日付別に上書きしていない日は、このテンプレートが使われます。")
        if "template_weekday" not in st.session_state:
            st.session_state.template_weekday = datetime.now().weekday()
        template_weekday = st.selectbox(
            "曜日を選択",
            range(7),
            format_func=lambda i: WEEKDAY_NAMES[i] + "曜日",
            key="template_weekday",
        )
        template_existing = get_demand_template_for_weekday(template_weekday)
        d_min, d_tgt, d_max = st.session_state.default_min, st.session_state.default_target, st.session_state.default_max
        with st.form("demand_template_form"):
            cols_header = st.columns([2, 1, 1, 1])
            cols_header[0].write("**スロット**")
            cols_header[1].write("**最小**")
            cols_header[2].write("**目標**")
            cols_header[3].write("**最大**")
            for slot in TIME_OPTIONS:
                m, t, mx = template_existing.get(slot, (d_min, d_tgt, d_max))
                k_m = f"tmpl_min_{template_weekday}_{slot}"
                k_t = f"tmpl_target_{template_weekday}_{slot}"
                k_x = f"tmpl_max_{template_weekday}_{slot}"
                cols = st.columns([2, 1, 1, 1])
                cols[0].write(slot)
                cols[1].number_input("最小", min_value=0, key=k_m, value=m, label_visibility="collapsed")
                cols[2].number_input("目標", min_value=0, key=k_t, value=t, label_visibility="collapsed")
                cols[3].number_input("最大", min_value=0, key=k_x, value=mx, label_visibility="collapsed")
            if st.form_submit_button("💾 この曜日のテンプレートを保存"):
                for slot in TIME_OPTIONS:
                    vm = st.session_state.get(f"tmpl_min_{template_weekday}_{slot}", d_min)
                    vt = st.session_state.get(f"tmpl_target_{template_weekday}_{slot}", d_tgt)
                    vx = st.session_state.get(f"tmpl_max_{template_weekday}_{slot}", d_max)
                    save_demand_template_slot(template_weekday, slot, vm, vt, vx)
                st.success(f"**{WEEKDAY_NAMES[template_weekday]}曜日** のテンプレートを保存しました。")
                st.rerun()

    st.markdown("---")
    st.markdown("**日付別の必要人数**")
    if "demand_date" not in st.session_state:
        st.session_state.demand_date = datetime.now().date()
    demand_date = st.date_input("日付を選択", key="demand_date")
    demand_date_str = demand_date.strftime("%Y-%m-%d")
    effective_demand, source = get_effective_demand_for_date(demand_date_str, TIME_OPTIONS)

    if source == "override":
        st.info("⚠️ **特定日の上書き設定中** — この日付専用のデータを表示しています。保存すると上書きが更新されます。")
    else:
        st.success("💡 **デフォルト適用中** — 曜日テンプレートまたは初期値です。保存するとこの日付専用の上書きとして記録されます。")

    d_min, d_tgt, d_max = st.session_state.default_min, st.session_state.default_target, st.session_state.default_max
    st.caption(f"一括で「最小{d_min}・目標{d_tgt}・最大{d_max}」を全スロットに反映します。（サイドバーで変更可）")
    if st.button("📋 全スロットにデフォルト値を一括適用", key="default_demand_btn"):
        apply_default_demand(demand_date_str, d_min, d_tgt, d_max)
        date_label = f"{demand_date.month}月{demand_date.day}日"
        st.success(f"**{date_label}** の必要人数を一括で設定しました。")
        st.rerun()

    with st.form("demand_form"):
        st.caption(f"**{demand_date_str}** の各スロットの必要人数を入力し、保存を押すとこの日付専用の上書きとして保存されます。")
        cols_header = st.columns([2, 1, 1, 1])
        cols_header[0].write("**スロット**")
        cols_header[1].write("**最小**")
        cols_header[2].write("**目標**")
        cols_header[3].write("**最大**")

        slot_values = []
        for slot in TIME_OPTIONS:
            min_def, target_def, max_def = effective_demand.get(slot, (2, 3, 4))
            k_min = f"min_{demand_date_str}_{slot}"
            k_target = f"target_{demand_date_str}_{slot}"
            k_max = f"max_{demand_date_str}_{slot}"
            if k_min not in st.session_state:
                st.session_state[k_min] = min_def
            if k_target not in st.session_state:
                st.session_state[k_target] = target_def
            if k_max not in st.session_state:
                st.session_state[k_max] = max_def
            cols = st.columns([2, 1, 1, 1])
            cols[0].write(slot)
            min_val = cols[1].number_input("最小", min_value=0, key=k_min, label_visibility="collapsed")
            target_val = cols[2].number_input("目標", min_value=0, key=k_target, label_visibility="collapsed")
            max_val = cols[3].number_input("最大", min_value=0, key=k_max, label_visibility="collapsed")
            slot_values.append((slot, min_val, target_val, max_val))

        demand_saved = st.form_submit_button("💾 保存（この日付の上書きとして保存）")
        if demand_saved:
            for slot, min_val, target_val, max_val in slot_values:
                save_demand_slot(demand_date_str, slot, min_val, target_val, max_val)
            st.success("この日付の必要人数を上書き保存しました。")
            st.rerun()

# ---------- タブ3: 希望シフト入力 ----------
with tab3:
    # カレンダーをタブ先頭に配置し、タブ選択中は常に表示されるようにする
    st.subheader("希望シフト一覧（カレンダー）")
    avail_list_cal = get_availabilities_with_attributes()
    calendar_events = build_calendar_events_for_lib(avail_list_cal)

    calendar_ok = False
    if st_calendar is not None:
        try:
            from datetime import date as date_type
            today = date_type.today()
            initial_date = today.strftime("%Y-%m-%d")
            calendar_options = {
                "editable": False,
                "selectable": True,
                "navLinks": True,
                "headerToolbar": {
                    "left": "prev,next today",
                    "center": "title",
                    "right": "dayGridMonth,timeGridWeek",
                },
                "initialDate": initial_date,
                "initialView": "dayGridMonth",
                "eventDisplay": "block",
                "dayMaxEvents": 8,
            }
            custom_css = """
            .fc-event-past { opacity: 0.7; }
            .fc-toolbar-title { font-size: 1.1rem; }
            .fc-daygrid-event { white-space: normal; }
            """
            with st.container():
                cal = st_calendar(
                    events=calendar_events,
                    options=calendar_options,
                    custom_css=custom_css,
                    key="hope_shift_calendar",
                )
            calendar_ok = True
        except Exception as e:
            st.warning(f"カレンダーコンポーネントの表示に失敗しました（{e}）。下記のタイムラインで表示します。")

    if not calendar_ok:
        if avail_list_cal:
            fig_cal = build_availability_calendar_figure(avail_list_cal)
            if fig_cal is not None:
                st.plotly_chart(fig_cal, use_container_width=True)
        else:
            st.info("まだ希望シフトが登録されていません。カレンダーに表示するには希望を登録してください。")
    if calendar_ok or avail_list_cal:
        st.caption("凡例: 青＝キーマン、緑＝新人、紫＝キーマン・新人、灰＝一般。")

    st.markdown("---")
    st.subheader("希望シフト登録（一括入力）")
    st.caption("日付を選ぶと全スタッフの希望を一覧で編集できます。フル・休みボタンや前日コピーで素早く入力し、最後に一括保存してください。")
    employees = get_employees()

    if not employees:
        st.info("希望シフトを登録するには、先に「1. スタッフ管理」でスタッフを登録してください。")
    else:
        # コールバックでフラグが立っていたらここで rerun（pending 適用のため）
        if st.session_state.get("avail_need_rerun"):
            del st.session_state.avail_need_rerun
            st.rerun()

        # 1. 日付を session_state から取得（ウィジェット表示前に確定）
        if "avail_edit_date" not in st.session_state:
            st.session_state.avail_edit_date = datetime.now().date()
        # 前日コピー押下時に退避した日付があれば復元（「編集する日付」が変わらないようにする）
        if "avail_edit_date_preserve" in st.session_state:
            st.session_state.avail_edit_date = st.session_state.avail_edit_date_preserve
            del st.session_state.avail_edit_date_preserve
        avail_date_str = st.session_state.avail_edit_date.strftime("%Y-%m-%d")

        # 2. pending を適用（すべての selectbox 表示「前」に実行）
        if "avail_pending" in st.session_state:
            action, date_str, emp_id = st.session_state.avail_pending
            if action == "copy_prev" and "avail_pending_prev_data" in st.session_state:
                prev_data = st.session_state.avail_pending_prev_data
                from datetime import timedelta
                prev_date = (datetime.strptime(date_str, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")
                st.session_state.avail_copied_msg = prev_date
                for eid, _n, _, _ in employees:
                    sk = f"avail_start_{date_str}_{eid}"
                    ek = f"avail_end_{date_str}_{eid}"
                    if eid in prev_data:
                        st.session_state[sk] = prev_data[eid][0]
                        st.session_state[ek] = prev_data[eid][1]
                    else:
                        st.session_state[sk] = ""
                        st.session_state[ek] = ""
                del st.session_state.avail_pending_prev_data
            elif action == "full" and emp_id is not None:
                sk = f"avail_start_{date_str}_{emp_id}"
                ek = f"avail_end_{date_str}_{emp_id}"
                st.session_state[sk] = "17:00"
                st.session_state[ek] = "29:00"
                st.session_state.avail_skip_key_init = True  # 他スタッフの未保存入力がDBで上書きされないようにする
            elif action == "off" and emp_id is not None:
                sk = f"avail_start_{date_str}_{emp_id}"
                ek = f"avail_end_{date_str}_{emp_id}"
                st.session_state[sk] = ""
                st.session_state[ek] = ""
                st.session_state.avail_skip_key_init = True
            del st.session_state.avail_pending

        # 3. 既存データ取得と OPTIONS
        existing_by_emp = get_availabilities_for_date_by_employee(avail_date_str)
        OPTIONS_AVAIL = [""] + TIME_OPTIONS  # "" = 休み

        # 4. キー初期化（ループの外側で一括。存在しない場合のみセット）
        # フル/休みボタン適用直後は他スタッフの入力がDBで上書きされないようスキップ
        if not st.session_state.pop("avail_skip_key_init", False):
            for emp_id, name, is_key_person, is_newbie in employees:
                sk = f"avail_start_{avail_date_str}_{emp_id}"
                ek = f"avail_end_{avail_date_str}_{emp_id}"
                if sk not in st.session_state:
                    s, e = existing_by_emp.get(emp_id, ("", ""))
                    st.session_state[sk] = s if s in OPTIONS_AVAIL else ""
                if ek not in st.session_state:
                    s, e = existing_by_emp.get(emp_id, ("", ""))
                    st.session_state[ek] = e if e in OPTIONS_AVAIL else ""

        # 5. ここからウィジェット表示（日付・メッセージ・ボタン・テーブル）
        avail_edit_date = st.date_input("編集する日付", key="avail_edit_date", help="この日付の希望を一括で編集します")
        avail_date_str = avail_edit_date.strftime("%Y-%m-%d")

        if st.session_state.get("avail_copied_msg"):
            prev_date = st.session_state.avail_copied_msg
            del st.session_state.avail_copied_msg
            st.success(f"前日（{prev_date}）の希望を入力欄に反映しました。必要に応じて編集してから一括保存してください。")

        st.button(
            "📅 前日の希望をコピー",
            key="copy_prev_avail",
            on_click=_avail_copy_prev_callback,
            help="選択中の日付の前日の希望を入力欄に反映します。",
        )

        st.markdown("---")
        h1, h2, h3, h4, h5 = st.columns([2, 1.2, 1.2, 0.8, 0.8])
        h1.markdown("**スタッフ**")
        h2.markdown("**開始時間**")
        h3.markdown("**終了時間**")
        h4.markdown("**クイック**")
        h5.markdown("")

        for emp_id, name, is_key_person, is_newbie in employees:
            sk = f"avail_start_{avail_date_str}_{emp_id}"
            ek = f"avail_end_{avail_date_str}_{emp_id}"
            start_val = st.session_state.get(sk, "")
            end_val = st.session_state.get(ek, "")
            filled = bool(start_val and end_val)

            c1, c2, c3, c4 = st.columns([2, 1.2, 1.2, 1.5])
            with c1:
                status = "✅ " if filled else "⏳ "
                badge = " キーマン" if is_key_person else (" 新人" if is_newbie else "")
                st.markdown(f"{status}**{name}**{badge}")
            with c2:
                st.selectbox("開始", options=OPTIONS_AVAIL, key=sk, label_visibility="collapsed")
            with c3:
                st.selectbox("終了", options=OPTIONS_AVAIL, key=ek, label_visibility="collapsed")
            with c4:
                col_f, col_o = st.columns(2)
                with col_f:
                    st.button(
                        "フル",
                        key=f"btn_full_{avail_date_str}_{emp_id}",
                        on_click=_avail_full_callback,
                        args=(avail_date_str, emp_id),
                        help="17:00〜29:00 をセット",
                    )
                with col_o:
                    st.button(
                        "休み",
                        key=f"btn_off_{avail_date_str}_{emp_id}",
                        on_click=_avail_off_callback,
                        args=(avail_date_str, emp_id),
                        help="リセット",
                    )

        st.markdown("---")
        if st.button("💾 この日の全データを一括保存", type="primary", key="bulk_save_avail"):
            # 保存時は「編集する日付」の現在値を必ず session_state から取得（rerun 順序で古い日付になるのを防ぐ）
            save_date = st.session_state.get("avail_edit_date", datetime.now().date())
            if hasattr(save_date, "strftime"):
                save_date_str = save_date.strftime("%Y-%m-%d")
            else:
                save_date_str = str(save_date)[:10]
            to_save = []
            for emp_id, name, _, _ in employees:
                sk = f"avail_start_{save_date_str}_{emp_id}"
                ek = f"avail_end_{save_date_str}_{emp_id}"
                s = st.session_state.get(sk)
                e = st.session_state.get(ek)
                if s is None:
                    s = ""
                if e is None:
                    e = ""
                s, e = str(s).strip(), str(e).strip()
                if s and e:
                    to_save.append((emp_id, s, e))
            save_availabilities_for_date(save_date_str, to_save)
            # 保存後 rerun するとメッセージが消えるため、session_state に記録して次回表示
            st.session_state.avail_save_success = (save_date_str, len(to_save))
            st.rerun()

        # 一括保存後の成功メッセージ（rerun 後も表示されるようにここで表示）
        if st.session_state.get("avail_save_success"):
            save_date, save_count = st.session_state.avail_save_success
            del st.session_state.avail_save_success
            st.success(f"**{save_date}** の希望を **{save_count}** 件保存しました。")

    # バックアップ: カレンダーが使えずデータがある場合は表形式も表示
    if not calendar_ok and avail_list_cal:
        with st.expander("📋 週刊スケジュール（表形式）"):
            df_backup = pd.DataFrame(
                [(date_str, name, f"{start_d}～{end_d}") for name, date_str, start_d, end_d, _, _ in avail_list_cal],
                columns=["日付", "スタッフ", "時間帯"],
            )
            st.dataframe(df_backup, use_container_width=True, hide_index=True)

    with st.expander("📋 希望シフト一覧（リスト）"):
        availabilities = get_availabilities()
        if availabilities:
            for av_id, name, date, start, end in availabilities:
                st.write(f"- **{name}** … {date} {to_display_time(start)} ～ {to_display_time(end)}")
        else:
            st.write("登録がありません。")

# ---------- タブ4: シフト自動生成 ----------
with tab4:
    st.subheader("シフト自動生成")
    st.caption("期間を選んで「シフト自動生成」を押すと、その期間の各日について希望・必要人数・キーマン・新人制限などのルールに沿ってシフト案を自動作成します。（最低勤務時間・新人上限はサイドバーで変更可）")
    if "gen_start_date" not in st.session_state:
        st.session_state.gen_start_date = datetime.now().date()
    if "gen_end_date" not in st.session_state:
        st.session_state.gen_end_date = datetime.now().date()

    col_start, col_end = st.columns(2)
    with col_start:
        gen_start_date = st.date_input("開始日", key="gen_start_date", help="生成する期間の開始日")
    with col_end:
        gen_end_date = st.date_input("終了日", key="gen_end_date", help="生成する期間の終了日（この日を含む）")

    if gen_start_date > gen_end_date:
        st.warning("開始日が終了日より後になっています。開始日 ≦ 終了日になるように選んでください。")
    else:
        days_count = (gen_end_date - gen_start_date).days + 1
        st.caption(f"**{gen_start_date.strftime('%Y-%m-%d')}** ～ **{gen_end_date.strftime('%Y-%m-%d')}** の **{days_count}** 日分を生成します。")

    if st.button("🪄 シフト自動生成", key="gen_btn", disabled=(gen_start_date > gen_end_date)):
        date_list = []
        d = gen_start_date
        while d <= gen_end_date:
            date_list.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)

        gen_results = {}
        progress = st.progress(0.0, text="生成中…")
        for i, date_str in enumerate(date_list):
            progress.progress((i + 1) / len(date_list), text=f"{date_str} を生成中…")
            success, assign_matrix, staff_list, error_message = solve_shift(
                date_str,
                min_work_hours=st.session_state.min_work_hours,
                newbie_max_per_slot=st.session_state.newbie_max_per_slot,
            )
            diag = diagnose_shift_failure(date_str) if not success else None
            gen_results[date_str] = (success, assign_matrix, staff_list, error_message, diag)
        progress.progress(1.0, text="完了")
        time.sleep(0.3)
        progress.empty()
        st.session_state.gen_results = gen_results
        st.session_state.gen_result_dates = date_list
        st.rerun()

    if st.session_state.get("gen_results") and st.session_state.get("gen_result_dates"):
        gen_results = st.session_state.gen_results
        gen_result_dates = st.session_state.gen_result_dates
        ok_count = sum(1 for d in gen_result_dates if gen_results[d][0])
        fail_count = len(gen_result_dates) - ok_count
        st.success(f"生成完了: 成功 **{ok_count}** 日、失敗 **{fail_count}** 日。表示する週を選んで週間シフト表を確認できます。")

        # 生成結果から「週」（月〜日）の選択肢を算出
        week_starts = sorted(set(
            (datetime.strptime(d, "%Y-%m-%d").date() - timedelta(days=datetime.strptime(d, "%Y-%m-%d").weekday())
             for d in gen_result_dates)
        ))
        week_options = week_starts
        week_labels = [f"{ws} ～ {(ws + timedelta(days=6)).strftime('%Y-%m-%d')}（{ws.month}/{ws.day}～）" for ws in week_options]

        if not week_options:
            st.info("表示する週がありません。")
        else:
            sel_idx = st.selectbox(
                "表示する週",
                range(len(week_options)),
                format_func=lambda i: week_labels[i] if i < len(week_labels) else "",
                key="gen_week_sel_idx",
            )
            if sel_idx is not None and sel_idx < len(week_options):
                week_start = week_options[sel_idx]
                week_dates = [(week_start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

                st.markdown("**週間シフト表**")
                fig_week = build_weekly_shift_figure(gen_results, week_dates, TIME_OPTIONS)
                st.plotly_chart(fig_week, use_container_width=True)
                st.caption("凡例: 青＝キーマン、緑＝新人、紫＝キーマン・新人、灰＝一般。横軸は17:00〜29:00・1時間刻み。")

        if fail_count > 0:
            with st.expander("⚠️ 失敗した日の詳細", expanded=False):
                for d in gen_result_dates:
                    success, _, _, error_message, diag = gen_results[d]
                    if not success:
                        st.markdown(f"**{d}**")
                        if diag:
                            st.dataframe(
                                pd.DataFrame(diag, columns=["時間帯", "原因"]),
                                use_container_width=True,
                                hide_index=True,
                            )
                        else:
                            st.caption(str(error_message))
                        st.info(
                            "必要人数設定で該当時間を0人にする（早締め）か、スタッフに希望追加を依頼してください。"
                        )
                        st.markdown("---")
