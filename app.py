import streamlit as st
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# 接続URLは st.secrets["DATABASE_URL"] で読み込む（PostgreSQL/Supabase用, GitHubに上げてもパスワードが公開されない）
def _get_database_url() -> str:
    try:
        url = st.secrets["DATABASE_URL"]
    except (KeyError, FileNotFoundError):
        raise RuntimeError(
            "StreamlitのSecretsに DATABASE_URL を設定してください。"
            " Streamlit Cloud の Secrets または .streamlit/secrets.toml に DATABASE_URL を追加してください。"
        )
    # 環境によって 'postgres://' で始まることがあるので、'postgresql://' に変換
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

# PostgreSQL用テーブル作成・接続
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

# あとはそのまま（CP-SAT, UI部分）…
