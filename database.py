# database.py
import logging
import aiosqlite
import numpy as np
import calendar
from async_lru import alru_cache
from datetime import datetime, date, time, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
from config import DB_NAME, LOCAL_TIMEZONE

logger = logging.getLogger(__name__)

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                telegram_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                face_encoding BLOB
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_telegram_id INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                FOREIGN KEY (employee_telegram_id) REFERENCES employees (telegram_id),
                UNIQUE(employee_telegram_id, day_of_week)
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS check_ins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_telegram_id INTEGER,
                timestamp DATETIME DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%S', 'NOW')),
                check_in_type TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                distance_meters REAL,
                face_similarity REAL,
                status TEXT NOT NULL,
                FOREIGN KEY (employee_telegram_id) REFERENCES employees (telegram_id)
            );
        """)
        await db.commit()
        logger.info("База данных инициализирована.")


async def get_employee_data(telegram_id: int, include_inactive=False) -> dict | None:
    # ... (скопируйте сюда содержимое функции get_employee_data из bot.py)
    sql = "SELECT telegram_id, full_name, face_encoding, is_active FROM employees WHERE telegram_id = ?"
    if not include_inactive:
        sql += " AND is_active = TRUE"
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(sql, (telegram_id,))
        row = await cursor.fetchone()
        if row:
            return {"id": row[0], "name": row[1], "face_encoding": row[2], "is_active": row[3] == 1}
    return None

@alru_cache(maxsize=128)
async def get_all_active_employees_with_schedules(for_day: int) -> list:
    # ... (скопируйте сюда содержимое функции get_all_active_employees_with_schedules из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        query = """
            SELECT e.telegram_id, e.full_name, s.start_time
            FROM employees e
            JOIN schedules s ON e.telegram_id = s.employee_telegram_id
            WHERE e.is_active = TRUE AND s.day_of_week = ?
        """
        cursor = await db.execute(query, (for_day,))
        return await cursor.fetchall()

@alru_cache(maxsize=256)
async def get_employee_today_schedule(telegram_id: int) -> dict | None:
    # ... (скопируйте сюда содержимое функции get_employee_today_schedule из bot.py)
    today_weekday = datetime.now(LOCAL_TIMEZONE).weekday()
    async with aiosqlite.connect(DB_NAME) as db:
        query = "SELECT e.full_name, s.start_time, s.end_time FROM employees e JOIN schedules s ON e.telegram_id = s.employee_telegram_id WHERE e.telegram_id = ? AND s.day_of_week = ? AND e.is_active = TRUE"
        cursor = await db.execute(query, (telegram_id, today_weekday))
        row = await cursor.fetchone()
        if row: return {"name": row[0], "start_time": time.fromisoformat(row[1]), "end_time": time.fromisoformat(row[2])}
    return None

async def has_checked_in_today(telegram_id: int, check_in_type: str) -> bool:
    # ... (скопируйте сюда содержимое функции has_checked_in_today из bot.py)
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    start_of_day_local = datetime.combine(today_local, time.min, tzinfo=LOCAL_TIMEZONE)
    end_of_day_local = datetime.combine(today_local, time.max, tzinfo=LOCAL_TIMEZONE)

    start_of_day_utc = start_of_day_local.astimezone(ZoneInfo("UTC"))
    end_of_day_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))

    statuses_to_check = ('SUCCESS', 'LATE') if check_in_type == 'ARRIVAL' else ('SUCCESS',)
    
    async with aiosqlite.connect(DB_NAME) as db:
        query = """
            SELECT 1 FROM check_ins 
            WHERE employee_telegram_id = ? 
              AND check_in_type = ? 
              AND status IN ({seq})
              AND timestamp BETWEEN ? AND ?
            LIMIT 1
        """.format(seq=','.join('?' for _ in statuses_to_check))
        
        params = (
            telegram_id, 
            check_in_type, 
            *statuses_to_check, 
            start_of_day_utc.strftime('%Y-%m-%d %H:%M:%S'), 
            end_of_day_utc.strftime('%Y-%m-%d %H:%M:%S')
        )
        cursor = await db.execute(query, params)
        return await cursor.fetchone() is not None

async def set_employee_active_status(telegram_id: int, is_active: bool):
    # ... (скопируйте сюда содержимое функции set_employee_active_status из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE employees SET is_active = ? WHERE telegram_id = ?", (is_active, telegram_id))
        await db.commit()

async def set_face_encoding(telegram_id: int, encoding: np.ndarray):
    # ... (скопируйте сюда содержимое функции set_face_encoding из bot.py)
    encoding_bytes = encoding.tobytes()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE employees SET face_encoding = ? WHERE telegram_id = ?", (encoding_bytes, telegram_id))
        await db.commit()

async def add_or_update_employee(telegram_id: int, full_name: str, schedule_data: dict):
    # ... (скопируйте сюда содержимое функции add_or_update_employee из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO employees (telegram_id, full_name, is_active) VALUES (?, ?, TRUE)", (telegram_id, full_name))
        await db.execute("UPDATE employees SET full_name = ?, is_active = TRUE WHERE telegram_id = ?", (full_name, telegram_id))
        await db.execute("DELETE FROM schedules WHERE employee_telegram_id = ?", (telegram_id,))
        for day_of_week, times in schedule_data.items():
            if times:
                await db.execute("INSERT INTO schedules (employee_telegram_id, day_of_week, start_time, end_time) VALUES (?, ?, ?, ?)", (telegram_id, day_of_week, times['start'], times['end']))
        await db.commit()
    logger.info("Обновление данных сотрудника -> очистка кэша расписаний.")
    get_all_active_employees_with_schedules.cache_clear()
    get_employee_today_schedule.cache_clear()

async def log_check_in_attempt(telegram_id: int, check_in_type: str, status: str, lat=None, lon=None, distance=None, similarity=None):
    # ... (скопируйте сюда содержимое функции log_check_in_attempt из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        # Сохраняем время в UTC, как и раньше
        timestamp_utc = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        await db.execute("INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status, latitude, longitude, distance_meters, face_similarity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                         (timestamp_utc, telegram_id, check_in_type, status, lat, lon, distance, similarity))
        await db.commit()

async def get_report_stats_for_period(start_date: date, end_date: date) -> dict:
    # ... (скопируйте сюда содержимое функции get_report_stats_for_period из bot.py)
    stats = {
        'total_work_days': 0, 
        'total_arrivals': 0, 
        'total_lates': 0, 
        'absences': defaultdict(list),
        'late_employees': defaultdict(list)
    }

    start_dt_local = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_dt_local = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_dt_utc = start_dt_local.astimezone(ZoneInfo("UTC"))
    end_dt_utc = end_dt_local.astimezone(ZoneInfo("UTC"))

    async with aiosqlite.connect(DB_NAME) as db:
        cursor_employees = await db.execute("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE")
        all_employees = {row[0]: row[1] for row in await cursor_employees.fetchall()}

        cursor_schedules = await db.execute("SELECT employee_telegram_id, day_of_week FROM schedules")
        schedules = defaultdict(set)
        for emp_id, day in await cursor_schedules.fetchall():
            schedules[emp_id].add(day)

        query = """
            SELECT employee_telegram_id, timestamp, status 
            FROM check_ins 
            WHERE check_in_type = 'ARRIVAL' 
              AND timestamp BETWEEN ? AND ?
        """
        params = (start_dt_utc.strftime('%Y-%m-%d %H:%M:%S'), end_dt_utc.strftime('%Y-%m-%d %H:%M:%S'))
        cursor_arrivals = await db.execute(query, params)
        
        arrivals_by_date = defaultdict(dict)
        for emp_id, ts_str, status in await cursor_arrivals.fetchall():
            utc_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo("UTC"))
            local_date_str = utc_dt.astimezone(LOCAL_TIMEZONE).date().isoformat()
            arrivals_by_date[local_date_str][emp_id] = status

        for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
            weekday = current_date.weekday()
            date_str = current_date.isoformat()
            
            for emp_id, name in all_employees.items():
                if weekday in schedules.get(emp_id, set()):
                    stats['total_work_days'] += 1
                    
                    if emp_id in arrivals_by_date.get(date_str, {}):
                        stats['total_arrivals'] += 1
                        if arrivals_by_date[date_str][emp_id] == 'LATE':
                            stats['total_lates'] += 1
                            stats['late_employees'][name].append(current_date.strftime('%d.%m'))
                    else:
                        if current_date <= datetime.now(LOCAL_TIMEZONE).date():
                            stats['absences'][name].append(current_date.strftime('%d.%m'))
    return stats


async def get_all_checkins_for_export() -> list:
    # ... (скопируйте сюда содержимое функции get_all_checkins_for_export из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        query = "SELECT c.timestamp, e.full_name, c.check_in_type, c.status, c.latitude, c.longitude, c.distance_meters, c.face_similarity FROM check_ins c JOIN employees e ON c.employee_telegram_id = e.telegram_id ORDER BY c.timestamp DESC"
        return await (await db.execute(query)).fetchall()


async def get_monthly_summary_data(year: int, month: int) -> list[list]:
    # ... (скопируйте сюда содержимое функции get_monthly_summary_data из bot.py)
    try:
        start_date = date(year, month, 1)
        num_days = calendar.monthrange(year, month)[1]
        end_date = date(year, month, num_days)
    except ValueError:
        logger.error(f"Неверный год или месяц: {year}-{month}")
        return []

    # --- 1. Получение всех необходимых данных ---
    
    # Конвертируем даты в UTC для запроса к БД
    start_dt_local = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_dt_local = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_dt_utc = start_dt_local.astimezone(ZoneInfo("UTC"))
    end_dt_utc = end_dt_local.astimezone(ZoneInfo("UTC"))

    all_employees = {}
    schedules = defaultdict(set)
    checkins = defaultdict(dict)

    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем сотрудников
        cursor_employees = await db.execute("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE ORDER BY full_name")
        for row in await cursor_employees.fetchall():
            all_employees[row[0]] = row[1]
        
        # Получаем все расписания
        cursor_schedules = await db.execute("SELECT employee_telegram_id, day_of_week FROM schedules")
        for emp_id, day in await cursor_schedules.fetchall():
            schedules[emp_id].add(day)

        # Получаем все чекины за месяц
        query = "SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE check_in_type = 'ARRIVAL' AND timestamp BETWEEN ? AND ?"
        params = (start_dt_utc.strftime('%Y-%m-%d %H:%M:%S'), end_dt_utc.strftime('%Y-%m-%d %H:%M:%S'))
        cursor_arrivals = await db.execute(query, params)
        for emp_id, ts_str, status in await cursor_arrivals.fetchall():
            utc_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo("UTC"))
            local_date_str = utc_dt.astimezone(LOCAL_TIMEZONE).date().isoformat()
            checkins[emp_id][local_date_str] = status

    # --- 2. Формирование таблицы ---

    # Заголовок таблицы (Сотрудник, 01.06, 02.06, ...)
    header = ["Сотрудник"] + [f"{day:02d}.{month:02d}" for day in range(1, num_days + 1)]
    result_table = [header]

    # Проходим по каждому сотруднику
    for emp_id, name in all_employees.items():
        employee_row = [name]
        # Проходим по каждому дню месяца
        for day in range(1, num_days + 1):
            current_date = date(year, month, day)
            current_date_str = current_date.isoformat()
            weekday = current_date.weekday()
            
            status_str = "Выходной" # Статус по умолчанию

            # Если день рабочий по графику
            if weekday in schedules.get(emp_id, set()):
                # Проверяем, был ли чекин в этот день
                checkin_status = checkins.get(emp_id, {}).get(current_date_str)
                if checkin_status:
                    if checkin_status == 'LATE':
                        status_str = 'Опоздал'
                    elif checkin_status == 'SUCCESS':
                        status_str = 'Вовремя'
                else:
                    # Если чекина не было, но день уже прошел или сегодня
                    if current_date <= datetime.now(LOCAL_TIMEZONE).date():
                        status_str = 'Пропустил'
                    else: # Для будущих рабочих дней
                        status_str = '—'

            employee_row.append(status_str)
        result_table.append(employee_row)

    return result_table