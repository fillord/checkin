import logging
import asyncpg
import numpy as np
import calendar
from datetime import datetime, date, time, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
from config import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, LOCAL_TIMEZONE

logger = logging.getLogger(__name__)

def _build_composite_status(status_list: list, is_work_day: bool, is_past_date: bool, is_holiday: bool) -> str:
    if is_holiday:
        return "Праздник"
    if not is_work_day:
        return "Выходной"
    if 'VACATION' in status_list:
        return 'Отпуск'
    if 'SICK_LEAVE' in status_list:
        return 'Больничный'
    if 'ABSENT' in status_list:
        return "Прогул"
    arrival_status = ""
    if 'LATE' in status_list:
        arrival_status = "Опоздал"
    elif 'SUCCESS' in status_list:
        arrival_status = "Вовремя"
    
    if not arrival_status:
        if is_past_date:
            return "Пропустил" 
        else:
            return "—"  

    departure_status = ""
    if 'APPROVED_LEAVE' in status_list:
        departure_status = "Отпросился"
    elif 'ABSENT_INCOMPLETE' in status_list:
        departure_status = "Не завершил день"
    final_parts = [arrival_status]
    if departure_status:
        final_parts.append(departure_status)
    return ", ".join(final_parts)

async def get_db_connection():
    return await asyncpg.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST
    )

async def init_db():
    conn = await get_db_connection()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                telegram_id BIGINT PRIMARY KEY,
                full_name TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                face_encoding BYTEA
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id SERIAL PRIMARY KEY,
                employee_telegram_id BIGINT NOT NULL,
                day_of_week INTEGER NOT NULL,
                start_time TIME,
                end_time TIME,
                effective_from_date DATE NOT NULL,
                FOREIGN KEY (employee_telegram_id) REFERENCES employees (telegram_id) ON DELETE CASCADE,
                UNIQUE(employee_telegram_id, day_of_week, effective_from_date)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS check_ins (
                id SERIAL PRIMARY KEY,
                employee_telegram_id BIGINT,
                timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'utc'),
                check_in_type TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                distance_meters REAL,
                face_similarity REAL,
                status TEXT NOT NULL,
                FOREIGN KEY (employee_telegram_id) REFERENCES employees (telegram_id) ON DELETE CASCADE
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS leaves (
                id SERIAL PRIMARY KEY,
                employee_telegram_id BIGINT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                leave_type TEXT NOT NULL, -- 'VACATION' или 'SICK_LEAVE'
                FOREIGN KEY (employee_telegram_id) REFERENCES employees (telegram_id) ON DELETE CASCADE
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS holidays (
                holiday_date DATE PRIMARY KEY,
                holiday_name TEXT NOT NULL
            );
        """)
        logger.info("База данных PostgreSQL инициализирована.")
    finally:
        await conn.close()

async def add_holiday(holiday_date: date, holiday_name: str):
    conn = await get_db_connection()
    try:
        await conn.execute(
            "INSERT INTO holidays (holiday_date, holiday_name) VALUES ($1, $2) ON CONFLICT (holiday_date) DO UPDATE SET holiday_name = $2",
            holiday_date, holiday_name
        )
    finally:
        await conn.close()

async def delete_holiday(holiday_date: date):
    conn = await get_db_connection()
    try:
        await conn.execute("DELETE FROM holidays WHERE holiday_date = $1", holiday_date)
    finally:
        await conn.close()

async def get_holidays_for_year(year: int) -> list[dict]:
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT holiday_date, holiday_name FROM holidays WHERE EXTRACT(YEAR FROM holiday_date) = $1 ORDER BY holiday_date", year)
        return [dict(row) for row in rows]
    finally:
        await conn.close()

async def is_holiday(target_date: date) -> bool:
    conn = await get_db_connection()
    try:
        result = await conn.fetchval("SELECT 1 FROM holidays WHERE holiday_date = $1", target_date)
        return result is not None
    finally:
        await conn.close()

async def get_employee_data(telegram_id, include_inactive=False):
    sql = "SELECT telegram_id, full_name, face_encoding, is_active FROM employees WHERE telegram_id = $1"
    if not include_inactive:
        sql += " AND is_active = TRUE"
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(sql, telegram_id)
        if row:
            return dict(row)
    finally:
        await conn.close()
    return None

async def get_employee_with_schedule(telegram_id: int) -> dict | None:
    conn = await get_db_connection()
    try:
        query = """
            SELECT e.telegram_id, e.full_name, MAX(s.effective_from_date) as last_effective_date
            FROM employees e
            LEFT JOIN schedules s ON e.telegram_id = s.employee_telegram_id
            WHERE e.telegram_id = $1 AND e.is_active = TRUE
            GROUP BY e.telegram_id, e.full_name
        """
        employee_info = await conn.fetchrow(query, telegram_id)
        if not employee_info:
            return None
        result = {
            'telegram_id': employee_info['telegram_id'],
            'full_name': employee_info['full_name'],
            'schedule': {},
            'schedule_effective_date': employee_info['last_effective_date'] 
        }
        if employee_info['last_effective_date']:
            schedule_query = """
                SELECT day_of_week, start_time, end_time 
                FROM schedules 
                WHERE employee_telegram_id = $1 AND effective_from_date = $2
            """
            schedule_rows = await conn.fetch(schedule_query, telegram_id, employee_info['last_effective_date'])
            schedule_map = {row['day_of_week']: row for row in schedule_rows}
            for i in range(7):
                row = schedule_map.get(i)
                if row and row['start_time'] and row['end_time']:
                    start_str = row['start_time'].strftime('%H:%M')
                    end_str = row['end_time'].strftime('%H:%M')
                    result['schedule'][i] = f"{start_str}-{end_str}"
                else:
                    result['schedule'][i] = "0"
        return result
    finally:
        await conn.close()

async def get_all_active_employees(search_query: str = None, sort_by: str = 'full_name', sort_order: str = 'asc') -> list[dict]:
    allowed_sort_columns = ['telegram_id', 'full_name']
    if sort_by not in allowed_sort_columns:
        sort_by = 'full_name'
    sort_order = 'DESC' if sort_order.lower() == 'desc' else 'ASC'
    query_params = []
    sql = "SELECT telegram_id, full_name, is_active FROM employees WHERE is_active = TRUE"
    if search_query:
        sql += " AND full_name ILIKE $1"
        query_params.append(f"%{search_query}%")

    sql += f" ORDER BY {sort_by} {sort_order}"
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(sql, *query_params)
        return [dict(row) for row in rows]
    finally:
        await conn.close()

async def is_employee_active(telegram_id: int) -> bool:
    """Проверяет, активен ли сотрудник в базе данных PostgreSQL."""
    conn = await get_db_connection()
    try:
        is_active = await conn.fetchval(
            "SELECT is_active FROM employees WHERE telegram_id = $1",
            telegram_id
        )
        return is_active if is_active is not None else False
    finally:
        await conn.close()

async def get_all_active_employees_with_schedules(for_date: date) -> list:
    query = """
        WITH latest_schedules AS (
            SELECT
                s.employee_telegram_id,
                s.start_time,
                ROW_NUMBER() OVER(PARTITION BY s.employee_telegram_id ORDER BY s.effective_from_date DESC) as rn
            FROM schedules s
            WHERE s.effective_from_date <= $1 AND s.day_of_week = $2
        )
        SELECT
            e.telegram_id,
            e.full_name,
            ls.start_time
        FROM employees e
        JOIN latest_schedules ls ON e.telegram_id = ls.employee_telegram_id
        WHERE e.is_active = TRUE AND ls.rn = 1 AND ls.start_time IS NOT NULL -- <-- ВОТ ГЛАВНОЕ ИСПРАВЛЕНИЕ
    """
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, for_date, for_date.weekday())
        return [tuple(row.values()) for row in rows]
    finally:
        await conn.close()

async def get_schedule_for_specific_date(conn, telegram_id, target_date):
    query = "SELECT start_time, end_time FROM schedules WHERE employee_telegram_id = $1 AND day_of_week = $2 AND effective_from_date <= $3 ORDER BY effective_from_date DESC LIMIT 1"
    row = await conn.fetchrow(query, telegram_id, target_date.weekday(), target_date)
    if row and row['start_time'] is not None and row['end_time'] is not None:
        return {"start_time": row['start_time'], "end_time": row['end_time']}
    return None

async def get_employee_today_schedule(telegram_id: int) -> dict | None:
    today = datetime.now(LOCAL_TIMEZONE).date()
    conn = await get_db_connection()
    try:
        emp_row = await conn.fetchrow("SELECT full_name FROM employees WHERE telegram_id = $1", telegram_id)
        if not emp_row: 
            return None

        schedule = await get_schedule_for_specific_date(conn, telegram_id, today)
        if schedule:
            schedule['name'] = emp_row['full_name']
            return schedule
    finally:
        await conn.close()
    return None

async def has_checked_in_on_date(telegram_id: int, check_in_type: str, for_date: date) -> bool:
    start_of_day_local = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_of_day_local = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_of_day_utc = start_of_day_local.astimezone(ZoneInfo("UTC"))
    end_of_day_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))

    statuses_to_check = ('SUCCESS', 'LATE') if check_in_type == 'ARRIVAL' else ('SUCCESS',)

    conn = await get_db_connection()
    try:
        query = """
            SELECT 1 FROM check_ins 
            WHERE employee_telegram_id = $1
              AND check_in_type = $2
              AND status = ANY($3)
              AND timestamp BETWEEN $4 AND $5
            LIMIT 1
        """
        row = await conn.fetchrow(query, telegram_id, check_in_type, list(statuses_to_check), start_of_day_utc, end_of_day_utc)
        return row is not None
    finally:
        await conn.close()

async def has_checked_in_today(telegram_id: int, check_in_type: str) -> bool:
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    return await has_checked_in_on_date(telegram_id, check_in_type, today_local)

async def is_day_finished_for_user(telegram_id: int) -> bool:
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    start_of_day_utc = datetime.combine(today_local, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
    end_of_day_utc = datetime.combine(today_local, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
    conn = await get_db_connection()
    try:
        leave_query = "SELECT 1 FROM leaves WHERE employee_telegram_id = $1 AND start_date <= $2 AND end_date >= $2 LIMIT 1"
        is_on_leave = await conn.fetchval(leave_query, telegram_id, today_local)
        if is_on_leave:
            return True
        statuses_to_check = ('SUCCESS', 'APPROVED_LEAVE')
        checkin_query = """
            SELECT 1 FROM check_ins 
            WHERE employee_telegram_id = $1
              AND (check_in_type = 'DEPARTURE' OR check_in_type = 'SYSTEM_LEAVE')
              AND status = ANY($2)
              AND timestamp BETWEEN $3 AND $4
            LIMIT 1
        """
        has_departed = await conn.fetchval(checkin_query, telegram_id, list(statuses_to_check), start_of_day_utc, end_of_day_utc)
        return has_departed is not None
    finally:
        await conn.close()

async def set_employee_active_status(telegram_id: int, is_active: bool):
    conn = await get_db_connection()
    try:
        await conn.execute(
            "UPDATE employees SET is_active = $1 WHERE telegram_id = $2",
            is_active, telegram_id
        )
    finally:
        await conn.close()

async def set_face_encoding(telegram_id: int, encoding: np.ndarray):
    encoding_bytes = encoding.tobytes()
    conn = await get_db_connection()
    try:
        await conn.execute(
            "UPDATE employees SET face_encoding = $1 WHERE telegram_id = $2",
            encoding_bytes, telegram_id
        )
    finally:
        await conn.close()

async def add_or_update_employee(telegram_id: int, full_name: str, schedule_data: dict, effective_date: date):
    conn = await get_db_connection()
    try:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO employees (telegram_id, full_name, is_active) VALUES ($1, $2, TRUE)
                ON CONFLICT (telegram_id) DO UPDATE SET full_name = $2, is_active = TRUE
                """,
                telegram_id, full_name
            )
            for day_of_week in range(7):
                times = schedule_data.get(day_of_week)
                start_time = times.get('start') if times else None
                end_time = times.get('end') if times else None
                await conn.execute(
                    """
                    INSERT INTO schedules (employee_telegram_id, day_of_week, effective_from_date, start_time, end_time)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (employee_telegram_id, day_of_week, effective_from_date) DO UPDATE SET
                        start_time = EXCLUDED.start_time,
                        end_time = EXCLUDED.end_time
                    """,
                    telegram_id, day_of_week, effective_date, start_time, end_time
                )
        logger.info(f"График для сотрудника {telegram_id} с {effective_date} успешно обновлен (метод ON CONFLICT).")
    finally:
        await conn.close()

async def log_check_in_attempt(telegram_id: int, check_in_type: str, status: str, lat=None, lon=None, distance=None, similarity=None):
    conn = await get_db_connection()
    try:
        await conn.execute(
            """
            INSERT INTO check_ins 
            (employee_telegram_id, check_in_type, status, latitude, longitude, distance_meters, face_similarity) 
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, 
            telegram_id, check_in_type, status, lat, lon, distance, similarity
        )
    finally:
        await conn.close()

async def override_as_absent(telegram_id: int, for_date: date):
    end_of_day_local = datetime.combine(for_date, time(23, 59, 59), tzinfo=LOCAL_TIMEZONE)
    timestamp_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))
    conn = await get_db_connection()
    try:
        await conn.execute(
            "INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status) VALUES ($1, $2, $3, $4)",
            timestamp_utc, telegram_id, 'SYSTEM', 'ABSENT_INCOMPLETE'
        )
        logger.info(f"Сотрудник {telegram_id} помечен как прогульщик (не отметил уход) за {for_date.isoformat()}")
    finally:
        await conn.close()

async def add_leave_period(telegram_id: int, start_date: date, end_date: date, leave_type: str):
    """Добавляет записи об отпуске/больничном в отдельную таблицу 'leaves'."""
    conn = await get_db_connection()
    try:
        leave_status = 'VACATION' if 'отпуск' in leave_type.lower() else 'SICK_LEAVE'
        await conn.execute(
            """
            INSERT INTO leaves (employee_telegram_id, start_date, end_date, leave_type)
            VALUES ($1, $2, $3, $4)
            """,
            telegram_id, start_date, end_date, leave_status
        )
        logger.info(f"Для сотрудника {telegram_id} назначен(а) {leave_type} с {start_date} по {end_date}.")
    finally:
        await conn.close()

async def get_all_checkins_for_export() -> list:
    conn = await get_db_connection()
    try:
        query = """
            SELECT c.timestamp, e.full_name, c.check_in_type, c.status, 
                   c.latitude, c.longitude, c.distance_meters, c.face_similarity 
            FROM check_ins c 
            JOIN employees e ON c.employee_telegram_id = e.telegram_id 
            ORDER BY c.timestamp DESC
        """
        rows = await conn.fetch(query)
        return [tuple(row.values()) for row in rows]
    finally:
        await conn.close()

async def get_report_stats_for_period(start_date: date, end_date: date) -> dict:
    stats = {
        'total_work_days': 0, 'total_arrivals': 0, 'total_lates': 0,
        'absences': defaultdict(list), 'late_employees': defaultdict(list)
    }
    conn = await get_db_connection()
    try:
        emp_rows = await conn.fetch("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE")
        all_employees = {row['telegram_id']: row['full_name'] for row in emp_rows}
        employee_ids = list(all_employees.keys())
        schedule_rows = await conn.fetch(
            """
            SELECT employee_telegram_id, day_of_week, effective_from_date, start_time
            FROM schedules
            WHERE employee_telegram_id = ANY($1)
            ORDER BY employee_telegram_id, effective_from_date DESC
            """,
            employee_ids
        )
        schedules_by_employee = defaultdict(list)
        for row in schedule_rows:
            schedules_by_employee[row['employee_telegram_id']].append(dict(row))
        def is_work_day_from_cache(emp_id, target_date, schedule_cache):
            for schedule_version in schedule_cache.get(emp_id, []):
                if schedule_version['effective_from_date'] <= target_date:
                    if schedule_version['day_of_week'] == target_date.weekday():
                        return schedule_version['start_time'] is not None
            return False
        start_dt_utc = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_dt_utc = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        event_rows = await conn.fetch(
            "SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2", 
            start_dt_utc, end_dt_utc
        )
        events_by_day = defaultdict(lambda: defaultdict(list))
        for row in event_rows:
            local_date = row['timestamp'].astimezone(LOCAL_TIMEZONE).date()
            events_by_day[local_date][row['employee_telegram_id']].append(row['status'])

        leaves_rows = await conn.fetch(
            "SELECT employee_telegram_id, start_date, end_date, leave_type FROM leaves WHERE start_date <= $1 AND end_date >= $2",
            end_date, start_date
        )
        leaves_by_day = defaultdict(lambda: defaultdict(str))
        for row in leaves_rows:
            current_date = row['start_date']
            while current_date <= row['end_date']:
                if start_date <= current_date <= end_date:
                    leaves_by_day[current_date][row['employee_telegram_id']] = row['leave_type']
                current_date += timedelta(days=1)
        for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
            for emp_id, name in all_employees.items():
                if not is_work_day_from_cache(emp_id, current_date, schedules_by_employee):
                    continue
                stats['total_work_days'] += 1
                if leaves_by_day.get(current_date, {}).get(emp_id):
                    continue
                day_events = events_by_day.get(current_date, {}).get(emp_id, [])
                if 'LATE' in day_events:
                    stats['total_arrivals'] += 1
                    stats['total_lates'] += 1
                    stats['late_employees'][name].append(current_date.strftime('%d.%m'))
                elif 'SUCCESS' in day_events:
                    stats['total_arrivals'] += 1
                else:
                    if current_date < datetime.now(LOCAL_TIMEZONE).date():
                        stats['absences'][name].append(current_date.strftime('%d.%m'))
    finally:
        await conn.close()
    return stats

async def cancel_leave_period(telegram_id: int, start_date: date, end_date: date) -> int:
    conn = await get_db_connection()
    try:
        query = """
            DELETE FROM leaves 
            WHERE employee_telegram_id = $1 
            AND start_date <= $2 AND end_date >= $3
        """
        status_str = await conn.execute(query, telegram_id, end_date, start_date)
        rows_deleted = int(status_str.split()[-1])
        logger.info(f"Для сотрудника {telegram_id} отменено отсутствие с {start_date} по {end_date}. Удалено записей: {rows_deleted}")
        return rows_deleted
    finally:
        await conn.close()

async def get_monthly_summary_data(year: int, month: int) -> list[list]:
    conn = await get_db_connection()
    try:
        start_date = date(year, month, 1)
        num_days = calendar.monthrange(year, month)[1]
        end_date = date(year, month, num_days)
        today = datetime.now(LOCAL_TIMEZONE).date()
        emp_rows = await conn.fetch("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE ORDER BY full_name")
        all_employees = {row['telegram_id']: row['full_name'] for row in emp_rows}
        employee_ids = list(all_employees.keys())
        holidays_rows = await conn.fetch("SELECT holiday_date FROM holidays WHERE holiday_date BETWEEN $1 AND $2", start_date, end_date)
        holidays_set = {row['holiday_date'] for row in holidays_rows}
        schedule_rows = await conn.fetch(
            """
            SELECT employee_telegram_id, day_of_week, effective_from_date, start_time
            FROM schedules
            WHERE employee_telegram_id = ANY($1)
            ORDER BY employee_telegram_id, effective_from_date DESC
            """,
            employee_ids
        )
        schedules_by_employee = defaultdict(list)
        for row in schedule_rows:
            schedules_by_employee[row['employee_telegram_id']].append(dict(row))
        start_dt_utc = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_dt_utc = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        event_rows = await conn.fetch("SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2", start_dt_utc, end_dt_utc)
        checkins = defaultdict(lambda: defaultdict(list))
        for row in event_rows:
            local_date_iso = row['timestamp'].astimezone(LOCAL_TIMEZONE).date().isoformat()
            checkins[row['employee_telegram_id']][local_date_iso].append(row['status'])
        leaves_rows = await conn.fetch("SELECT employee_telegram_id, start_date, end_date, leave_type FROM leaves WHERE start_date <= $1 AND end_date >= $2", end_date, start_date)
        for row in leaves_rows:
            current_date = row['start_date']
            while current_date <= row['end_date']:
                if start_date <= current_date <= end_date:
                     checkins[row['employee_telegram_id']][current_date.isoformat()].append(row['leave_type'])
                current_date += timedelta(days=1)
        def is_work_day_from_cache(emp_id, target_date, schedule_cache):
            for schedule_version in schedule_cache.get(emp_id, []):
                if schedule_version['effective_from_date'] <= target_date:
                    if schedule_version['day_of_week'] == target_date.weekday():
                        return schedule_version['start_time'] is not None
            return False
        header = ["Сотрудник"] + [f"{day:02d}.{month:02d}" for day in range(1, num_days + 1)]
        result_table = [header]
        for emp_id, name in all_employees.items():
            employee_row = [name]
            for day in range(1, num_days + 1):
                current_date = date(year, month, day)
                is_work_day = is_work_day_from_cache(emp_id, current_date, schedules_by_employee)
                status_list = checkins.get(emp_id, {}).get(current_date.isoformat(), [])
                final_status_str = _build_composite_status(
                    status_list, 
                    is_work_day=is_work_day,
                    is_past_date=(current_date <= today),
                    is_holiday=(current_date in holidays_set)
                )
                employee_row.append(final_status_str)
            result_table.append(employee_row)
        return result_table
    except ValueError:
        logger.error(f"Неверный год или месяц: {year}-{month}")
        return []
    finally:
        await conn.close()
async def get_employee_log(employee_id: int, start_date: date, end_date: date) -> list[dict]:
    start_dt_utc = datetime.combine(start_date, time.min).astimezone(ZoneInfo("UTC"))
    end_dt_utc = datetime.combine(end_date, time.max).astimezone(ZoneInfo("UTC"))
    conn = await get_db_connection()
    try:
        query = """
            SELECT timestamp, check_in_type, status, distance_meters, face_similarity
            FROM check_ins
            WHERE employee_telegram_id = $1 AND timestamp BETWEEN $2 AND $3
            ORDER BY timestamp DESC
        """
        rows = await conn.fetch(query, employee_id, start_dt_utc, end_dt_utc)
        log_entries = []
        for row in rows:
            local_ts = row['timestamp'].astimezone(LOCAL_TIMEZONE)
            entry = dict(row)
            entry['timestamp'] = local_ts.strftime('%d.%m.%Y %H:%M:%S')
            log_entries.append(entry)
        return log_entries
    finally:
        await conn.close()
async def bulk_add_or_update_schedules(schedules_data: list[dict]):
    conn = await get_db_connection()
    try:
        async with conn.transaction():
            for data in schedules_data:
                telegram_id = data['telegram_id']
                effective_date = data['effective_date']
                schedule = data['schedule']
                for day_of_week in range(7):
                    times = schedule.get(day_of_week)
                    start_time = times.get('start') if times else None
                    end_time = times.get('end') if times else None
                    await conn.execute(
                        """
                        INSERT INTO schedules (employee_telegram_id, day_of_week, effective_from_date, start_time, end_time)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (employee_telegram_id, day_of_week, effective_from_date) DO UPDATE SET
                            start_time = EXCLUDED.start_time,
                            end_time = EXCLUDED.end_time
                        """,
                        telegram_id, day_of_week, effective_date, start_time, end_time
                    )
        logger.info(f"Массовое обновление графиков завершено. Обработано записей: {len(schedules_data)}")
    finally:
        await conn.close()

async def get_personal_monthly_stats(employee_id: int) -> dict:
    now = datetime.now(LOCAL_TIMEZONE)
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_of_month_utc = start_of_month.astimezone(ZoneInfo("UTC"))
    now_utc = now.astimezone(ZoneInfo("UTC"))
    conn = await get_db_connection()
    try:
        query = """
            SELECT
                DATE_TRUNC('day', timestamp AT TIME ZONE $1) as checkin_day,
                status
            FROM check_ins
            WHERE employee_telegram_id = $2
              AND timestamp BETWEEN $3 AND $4
              AND status IN ('SUCCESS', 'LATE', 'APPROVED_LEAVE')
        """
        rows = await conn.fetch(query, LOCAL_TIMEZONE.key, employee_id, start_of_month_utc, now_utc)
        stats = {
            'work_days': set(),
            'late_days': set(),
            'left_early_days': set()
        }
        for row in rows:
            day = row['checkin_day'].date()
            status = row['status']
            if status == 'SUCCESS' or status == 'LATE':
                stats['work_days'].add(day)
            if status == 'LATE':
                stats['late_days'].add(day)
            if status == 'APPROVED_LEAVE':
                stats['left_early_days'].add(day)
        return {
            'work_days': len(stats['work_days']),
            'late_days': len(stats['late_days']),
            'left_early_days': len(stats['left_early_days'])
        }
    finally:
        await conn.close()

async def get_dashboard_stats(for_date: date) -> dict:
    """Собирает оперативную статистику за указанную дату для дашборда из PostgreSQL."""
    stats = {
        'total_scheduled': 0,
        'arrived': {},      
        'departed': {},     
        'on_leave': {},     
        'absent': {},       
        'incomplete': {}    
    }
    conn = await get_db_connection()
    try:
        query_employees = """
            WITH latest_schedules AS (
                SELECT
                    s.employee_telegram_id,
                    s.start_time,
                    ROW_NUMBER() OVER(PARTITION BY s.employee_telegram_id ORDER BY s.effective_from_date DESC) as rn
                FROM schedules s
                WHERE s.effective_from_date <= $1 AND s.day_of_week = $2
            )
            SELECT
                e.telegram_id,
                e.full_name
            FROM employees e
            JOIN latest_schedules ls ON e.telegram_id = ls.employee_telegram_id
            WHERE e.is_active = TRUE AND ls.rn = 1 AND ls.start_time IS NOT NULL
        """
        scheduled_rows = await conn.fetch(query_employees, for_date, for_date.weekday())
        scheduled_employees = {row['telegram_id']: row['full_name'] for row in scheduled_rows}
        stats['total_scheduled'] = len(scheduled_employees)
        stats['absent'] = scheduled_employees.copy()
        start_of_day_utc = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_of_day_utc = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        leaves_rows = await conn.fetch("SELECT employee_telegram_id, leave_type FROM leaves WHERE start_date <= $1 AND end_date >= $1", for_date)
        for row in leaves_rows:
            emp_id = row['employee_telegram_id']
            if emp_id in scheduled_employees:
                stats['on_leave'][emp_id] = {'name': scheduled_employees[emp_id], 'status': row['leave_type']}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
        query_events = "SELECT employee_telegram_id, check_in_type, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2"
        event_rows = await conn.fetch(query_events, start_of_day_utc, end_of_day_utc)
        arrivals = {}
        departures = set()
        for row in event_rows:
            emp_id = row['employee_telegram_id']
            check_type = row['check_in_type']
            status = row['status']
            if emp_id not in scheduled_employees: continue
            if emp_id in stats['on_leave']: continue
            if check_type == 'SYSTEM_LEAVE' and status == 'APPROVED_LEAVE':
                 departures.add(emp_id)
            elif check_type == 'ARRIVAL' and status in ('SUCCESS', 'LATE'):
                arrivals[emp_id] = {'name': scheduled_employees[emp_id], 'status': status}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
            elif check_type == 'DEPARTURE' and status == 'SUCCESS':
                departures.add(emp_id)
            elif check_type == 'SYSTEM' and status == 'ABSENT_INCOMPLETE':
                stats['incomplete'][emp_id] = scheduled_employees[emp_id]
                if emp_id in stats['absent']: del stats['absent'][emp_id]
        for emp_id, data in arrivals.items():
            if emp_id in stats['incomplete']:
                continue 
            if emp_id in departures:
                stats['departed'][emp_id] = data['name']
            else:
                stats['arrived'][emp_id] = data
    finally:
        await conn.close()
    
    return stats

async def mark_as_absent(telegram_id: int, for_date: date):
    """Marks an employee as absent by inserting a 'SYSTEM' check-in with 'ABSENT' status."""
    timestamp_utc = datetime.now(ZoneInfo("UTC"))
    conn = await get_db_connection()
    try:
        # Убедимся, что для этого пользователя нет других записей (приход, отпуск и т.д.) за этот день
        start_of_day_utc = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_of_day_utc = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))

        existing_record = await conn.fetchval("""
            SELECT 1 FROM check_ins
            WHERE employee_telegram_id = $1
            AND timestamp BETWEEN $2 AND $3
            AND status IN ('SUCCESS', 'LATE', 'ABSENT', 'APPROVED_LEAVE', 'VACATION', 'SICK_LEAVE')
            LIMIT 1
        """, telegram_id, start_of_day_utc, end_of_day_utc)
        
        existing_leave = await conn.fetchval("""
            SELECT 1 FROM leaves
            WHERE employee_telegram_id = $1 AND start_date <= $2 AND end_date >= $2
            LIMIT 1
        """, telegram_id, for_date)

        if not existing_record and not existing_leave:
            await conn.execute(
                """
                INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status)
                VALUES ($1, $2, 'SYSTEM', 'ABSENT')
                """,
                timestamp_utc, telegram_id,
            )
            logger.info(f"Сотрудник {telegram_id} помечен как 'прогул' за {for_date.isoformat()}")
    finally:
        await conn.close()