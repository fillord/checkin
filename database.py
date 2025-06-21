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
                start_time TEXT, -- ИЗМЕНЕНИЕ: Убрали NOT NULL
                end_time TEXT,   -- ИЗМЕНЕНИЕ: Убрали NOT NULL
                effective_from_date DATE NOT NULL,
                FOREIGN KEY (employee_telegram_id) REFERENCES employees (telegram_id),
                UNIQUE(employee_telegram_id, day_of_week, effective_from_date)
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
        try:
            cursor = await db.execute("PRAGMA table_info(schedules);")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'effective_from_date' not in columns:
                logger.info("Обнаружена старая структура таблицы schedules. Выполняется миграция...")
                await db.execute("ALTER TABLE schedules ADD COLUMN effective_from_date DATE;")
                # Заполняем поле датой по умолчанию для существующих графиков
                await db.execute("UPDATE schedules SET effective_from_date = '1970-01-01' WHERE effective_from_date IS NULL;")
                await db.commit()
                logger.info("Миграция таблицы schedules успешно завершена.")
        except Exception as e:
            logger.error(f"Ошибка при миграции таблицы schedules: {e}")
        
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

async def is_employee_active(telegram_id: int) -> bool:
    """Проверяет, активен ли сотрудник в базе данных."""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT is_active FROM employees WHERE telegram_id = ?",
            (telegram_id,)
        )
        row = await cursor.fetchone()
        # Возвращает True, если пользователь найден и его статус is_active = 1
        return row[0] if row else False
    
@alru_cache(maxsize=128)
async def get_all_active_employees_with_schedules(for_date: date) -> list:
    """Получает список активных сотрудников и их АКТУАЛЬНЫЙ на for_date график."""
    query = """
        WITH latest_schedules AS (
            SELECT
                s.employee_telegram_id,
                s.start_time,
                s.day_of_week,
                ROW_NUMBER() OVER(PARTITION BY s.employee_telegram_id, s.day_of_week ORDER BY s.effective_from_date DESC) as rn
            FROM schedules s
            WHERE s.effective_from_date <= ?
        )
        SELECT
            e.telegram_id,
            e.full_name,
            ls.start_time
        FROM employees e
        JOIN latest_schedules ls ON e.telegram_id = ls.employee_telegram_id
        WHERE e.is_active = TRUE AND ls.day_of_week = ? AND ls.rn = 1
    """
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(query, (for_date, for_date.weekday()))
        return await cursor.fetchall()

async def get_schedule_for_specific_date(db: aiosqlite.Connection, telegram_id: int, target_date: date) -> dict | None:
    """Получает корректную версию графика для сотрудника на КОНКРЕТНУЮ дату."""
    query = """
        SELECT start_time, end_time FROM schedules
        WHERE employee_telegram_id = ? AND day_of_week = ? AND effective_from_date <= ?
        ORDER BY effective_from_date DESC LIMIT 1
    """
    cursor = await db.execute(query, (telegram_id, target_date.weekday(), target_date))
    row = await cursor.fetchone()

    # Если запись найдена, но время NULL - это явный выходной
    if row and row[0] is not None and row[1] is not None:
        return {"start_time": time.fromisoformat(row[0]), "end_time": time.fromisoformat(row[1])}

    # Если запись не найдена или это явный выходной - возвращаем None
    return None

@alru_cache(maxsize=256)
async def get_employee_today_schedule(telegram_id: int) -> dict | None:
    """Получает актуальный график сотрудника на СЕГОДНЯ."""
    today = datetime.now(LOCAL_TIMEZONE).date()
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем имя сотрудника отдельно
        cursor = await db.execute("SELECT full_name FROM employees WHERE telegram_id = ?", (telegram_id,))
        emp_row = await cursor.fetchone()
        if not emp_row: return None
        
        schedule = await get_schedule_for_specific_date(db, telegram_id, today)
        if schedule:
            schedule['name'] = emp_row[0]
            return schedule
    return None

async def has_checked_in_on_date(telegram_id: int, check_in_type: str, for_date: date) -> bool:
    """Проверяет наличие чекина за конкретную дату."""
    start_of_day_local = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_of_day_local = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_of_day_utc = start_of_day_local.astimezone(ZoneInfo("UTC"))
    end_of_day_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))

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

async def is_day_finished_for_user(telegram_id: int) -> bool:
    """
    Проверяет, завершен ли рабочий день для сотрудника (был уход или системная отметка).
    """
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    start_of_day_local = datetime.combine(today_local, time.min, tzinfo=LOCAL_TIMEZONE)
    end_of_day_local = datetime.combine(today_local, time.max, tzinfo=LOCAL_TIMEZONE)

    start_of_day_utc = start_of_day_local.astimezone(ZoneInfo("UTC"))
    end_of_day_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))
    
    # Ищем либо успешный уход, либо одобренный системный уход
    statuses_to_check = ('SUCCESS', 'APPROVED_LEAVE', 'VACATION', 'SICK_LEAVE')
    
    async with aiosqlite.connect(DB_NAME) as db:
        query = f"""
            SELECT 1 FROM check_ins 
            WHERE employee_telegram_id = ? 
              AND (check_in_type = 'DEPARTURE' OR check_in_type = 'SYSTEM_LEAVE')
              AND status IN ({','.join('?'*len(statuses_to_check))})
              AND timestamp BETWEEN ? AND ?
            LIMIT 1
        """
        params = (
            telegram_id, 
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

# database.py

# database.py

async def add_or_update_employee(telegram_id: int, full_name: str, schedule_data: dict, effective_date: date):
    """
    Добавляет сотрудника или полностью перезаписывает версию его графика
    с указанной даты вступления в силу.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        # Обновляем данные самого сотрудника
        await db.execute("INSERT OR IGNORE INTO employees (telegram_id, full_name, is_active) VALUES (?, ?, TRUE)", (telegram_id, full_name))
        await db.execute("UPDATE employees SET full_name = ?, is_active = TRUE WHERE telegram_id = ?", (full_name, telegram_id))

        # --> ГЛАВНОЕ ИЗМЕНЕНИЕ: Перед вставкой новой версии графика, полностью удаляем старую для этой же effective_date
        logger.info(f"Очистка существующего графика для сотрудника {telegram_id} на дату {effective_date} перед записью новой версии.")
        await db.execute(
            "DELETE FROM schedules WHERE employee_telegram_id = ? AND effective_from_date = ?",
            (telegram_id, effective_date)
        )

        # Теперь вставляем 7 новых записей для всех дней недели
        for day_of_week in range(7):
            times = schedule_data.get(day_of_week)
            
            start_time = times.get('start') if times else None
            end_time = times.get('end') if times else None
            
            await db.execute(
                """
                INSERT INTO schedules (employee_telegram_id, day_of_week, start_time, end_time, effective_from_date)
                VALUES (?, ?, ?, ?, ?)
                """,
                (telegram_id, day_of_week, start_time, end_time, effective_date)
            )
        await db.commit()
    
    # Очищаем кэш, так как расписания были изменены
    get_all_active_employees_with_schedules.cache_clear()
    get_employee_today_schedule.cache_clear()
    logger.info(f"График для сотрудника {telegram_id} с {effective_date} успешно обновлен.")

async def log_check_in_attempt(telegram_id: int, check_in_type: str, status: str, lat=None, lon=None, distance=None, similarity=None):
    # ... (скопируйте сюда содержимое функции log_check_in_attempt из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        # Сохраняем время в UTC, как и раньше
        timestamp_utc = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        await db.execute("INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status, latitude, longitude, distance_meters, face_similarity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                         (timestamp_utc, telegram_id, check_in_type, status, lat, lon, distance, similarity))
        await db.commit()

async def override_as_absent(telegram_id: int, for_date: date):
    """Вставляет в БД системную запись о прогуле из-за отсутствия чекина ухода."""
    # Время в UTC, соответствующее концу дня по локальному времени
    end_of_day_local = datetime.combine(for_date, time(23, 59, 59), tzinfo=LOCAL_TIMEZONE)
    timestamp_utc = end_of_day_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status) VALUES (?, ?, ?, ?)",
            (timestamp_utc, telegram_id, 'SYSTEM', 'ABSENT_INCOMPLETE')
        )
        await db.commit()
        logger.info(f"Сотрудник {telegram_id} помечен как прогульщик (не отметил уход) за {for_date.isoformat()}")

# database.py

async def get_report_stats_for_period(start_date: date, end_date: date) -> dict:
    stats = {
        'total_work_days': 0, 'total_arrivals': 0, 'total_lates': 0,
        'absences': defaultdict(list), 'late_employees': defaultdict(list)
    }
    
    start_dt_local = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_dt_local = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_dt_utc = start_dt_local.astimezone(ZoneInfo("UTC"))
    end_dt_utc = end_dt_local.astimezone(ZoneInfo("UTC"))

    async with aiosqlite.connect(DB_NAME) as db:
        cursor_employees = await db.execute("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE")
        all_employees = {row[0]: row[1] for row in await cursor_employees.fetchall()}
        
        # Загружаем все события за период один раз
        query = "SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN ? AND ?"
        params = (start_dt_utc.strftime('%Y-%m-%d %H:%M:%S'), end_dt_utc.strftime('%Y-%m-%d %H:%M:%S'))
        cursor_events = await db.execute(query, params)
        
        events_by_date = defaultdict(dict)
        for emp_id, ts_str, status in await cursor_events.fetchall():
            utc_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo("UTC"))
            local_date = utc_dt.astimezone(LOCAL_TIMEZONE).date()
            if status == 'ABSENT_INCOMPLETE':
                events_by_date[local_date][emp_id] = 'ABSENT_INCOMPLETE'
            elif emp_id not in events_by_date.get(local_date, {}):
                events_by_date[local_date][emp_id] = status

        # --> ГЛАВНОЕ ИЗМЕНЕНИЕ: Проверяем график для каждого дня индивидуально
        for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
            for emp_id, name in all_employees.items():
                # Узнаем, работал ли сотрудник в ЭТОТ конкретный день по графику
                schedule_for_day = await get_schedule_for_specific_date(db, emp_id, current_date)
                
                if schedule_for_day: # Если день был рабочим
                    stats['total_work_days'] += 1
                    status = events_by_date.get(current_date, {}).get(emp_id)
                    
                    if status in ('VACATION', 'SICK_LEAVE', 'APPROVED_LEAVE'):
                        continue 
                    elif status == 'ABSENT_INCOMPLETE' or status is None:
                        if current_date <= datetime.now(LOCAL_TIMEZONE).date():
                           stats['absences'][name].append(current_date.strftime('%d.%m'))
                    elif status == 'LATE':
                        stats['total_arrivals'] += 1
                        stats['total_lates'] += 1
                        stats['late_employees'][name].append(current_date.strftime('%d.%m'))
                    elif status == 'SUCCESS':
                        stats['total_arrivals'] += 1
    return stats

async def get_all_checkins_for_export() -> list:
    # ... (скопируйте сюда содержимое функции get_all_checkins_for_export из bot.py)
    async with aiosqlite.connect(DB_NAME) as db:
        query = "SELECT c.timestamp, e.full_name, c.check_in_type, c.status, c.latitude, c.longitude, c.distance_meters, c.face_similarity FROM check_ins c JOIN employees e ON c.employee_telegram_id = e.telegram_id ORDER BY c.timestamp DESC"
        return await (await db.execute(query)).fetchall()

async def add_leave_period(telegram_id: int, start_date: date, end_date: date, leave_type: str):
    """Добавляет записи об отпуске/больничном для сотрудника на заданный период."""
    async with aiosqlite.connect(DB_NAME) as db:
        # Сначала получаем график сотрудника, чтобы не ставить отметки в его выходные
        cursor_schedules = await db.execute("SELECT day_of_week FROM schedules WHERE employee_telegram_id = ?", (telegram_id,))
        work_days = {row[0] for row in await cursor_schedules.fetchall()}
        
        leave_status = 'VACATION' if leave_type == 'Отпуск' else 'SICK_LEAVE'
        
        for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
            if current_date.weekday() in work_days:
                # Вставляем отметку на конец рабочего дня
                end_of_day_local = datetime.combine(current_date, time(23, 59, 58), tzinfo=LOCAL_TIMEZONE)
                timestamp_utc = end_of_day_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
                
                await db.execute(
                    "INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status) VALUES (?, ?, ?, ?)",
                    (timestamp_utc, telegram_id, 'SYSTEM_LEAVE', leave_status)
                )
        await db.commit()
        logger.info(f"Для сотрудника {telegram_id} назначен(а) {leave_type} с {start_date} по {end_date}")

async def cancel_leave_period(telegram_id: int, start_date: date, end_date: date) -> int:
    """Удаляет записи об отпуске/больничном для сотрудника на заданный период."""
    start_dt_local = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_dt_local = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_dt_utc_str = start_dt_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
    end_dt_utc_str = end_dt_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')

    leave_statuses = ('VACATION', 'SICK_LEAVE', 'APPROVED_LEAVE')
    
    query = f"""
        DELETE FROM check_ins 
        WHERE employee_telegram_id = ? 
        AND status IN ({','.join('?'*len(leave_statuses))})
        AND timestamp BETWEEN ? AND ?
    """
    params = (telegram_id, *leave_statuses, start_dt_utc_str, end_dt_utc_str)

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(query, params)
        await db.commit()
        logger.info(f"Для сотрудника {telegram_id} отменено отсутствие с {start_date} по {end_date}. Удалено записей: {cursor.rowcount}")
        return cursor.rowcount

# database.py

async def get_monthly_summary_data(year: int, month: int) -> list[list]:
    try:
        start_date = date(year, month, 1)
        num_days = calendar.monthrange(year, month)[1]
        end_date = date(year, month, num_days)
    except ValueError:
        logger.error(f"Неверный год или месяц: {year}-{month}")
        return []

    all_employees = {}
    checkins = defaultdict(dict)

    async with aiosqlite.connect(DB_NAME) as db:
        cursor_employees = await db.execute("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE ORDER BY full_name")
        all_employees = {row[0]: row[1] for row in await cursor_employees.fetchall()}
        
        # Загружаем все события за месяц
        start_dt_local = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE)
        end_dt_local = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE)
        start_dt_utc_str = start_dt_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        end_dt_utc_str = end_dt_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        
        query = "SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN ? AND ? ORDER BY CASE status WHEN 'ABSENT_INCOMPLETE' THEN 1 ELSE 2 END"
        params = (start_dt_utc_str, end_dt_utc_str)
        cursor_events = await db.execute(query, params)
        for emp_id, ts_str, status in await cursor_events.fetchall():
            utc_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo("UTC"))
            local_date_iso = utc_dt.astimezone(LOCAL_TIMEZONE).date().isoformat()
            if emp_id not in checkins or local_date_iso not in checkins[emp_id]:
                checkins[emp_id][local_date_iso] = status

        header = ["Сотрудник"] + [f"{day:02d}.{month:02d}" for day in range(1, num_days + 1)]
        result_table = [header]

        # --> ГЛАВНОЕ ИЗМЕНЕНИЕ: Проверяем график для каждого дня индивидуально
        for emp_id, name in all_employees.items():
            employee_row = [name]
            for day in range(1, num_days + 1):
                current_date = date(year, month, day)
                schedule_for_day = await get_schedule_for_specific_date(db, emp_id, current_date)
                
                status_str = "Выходной"
                if schedule_for_day: # Если день был рабочим
                    final_status = checkins.get(emp_id, {}).get(current_date.isoformat())
                    
                    if final_status == 'VACATION': status_str = 'Отпуск'
                    elif final_status == 'SICK_LEAVE': status_str = 'Больничный'
                    elif final_status == 'APPROVED_LEAVE': status_str = 'Отпросился'
                    elif final_status == 'ABSENT_INCOMPLETE': status_str = 'Прогул (нет ухода)'
                    elif final_status == 'LATE': status_str = 'Опоздал'
                    elif final_status == 'SUCCESS': status_str = 'Вовремя'
                    else:
                        if current_date <= datetime.now(LOCAL_TIMEZONE).date():
                            status_str = 'Пропустил'
                        else:
                            status_str = '—'
                
                employee_row.append(status_str)
            result_table.append(employee_row)

    return result_table

# database.py

async def get_dashboard_stats(for_date: date) -> dict:
    """Собирает оперативную статистику за указанную дату для дашборда."""
    stats = {
        'total_scheduled': 0,
        'arrived': {},  # {id: name, status: 'LATE'/'SUCCESS'}
        'departed': {}, # {id: name}
        'on_leave': {}, # {id: name, status: 'VACATION'/'SICK_LEAVE'/...}
        'absent': {},   # {id: name}
        'incomplete': {}# {id: name}
    }
    
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Получаем всех, кто должен работать сегодня
        cursor_employees = await db.execute(
            """
            SELECT e.telegram_id, e.full_name FROM employees e
            JOIN schedules s ON e.telegram_id = s.employee_telegram_id
            WHERE e.is_active = TRUE AND s.day_of_week = ?
            """,
            (for_date.weekday(),)
        )
        scheduled_employees = {row[0]: row[1] for row in await cursor_employees.fetchall()}
        stats['total_scheduled'] = len(scheduled_employees)
        
        # По умолчанию все, кто должен работать, - прогульщики
        stats['absent'] = scheduled_employees.copy()

        # 2. Получаем все события за сегодня
        start_of_day_local = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE)
        end_of_day_local = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE)
        start_dt_utc = start_of_day_local.astimezone(ZoneInfo("UTC"))
        end_dt_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))

        query = "SELECT employee_telegram_id, check_in_type, status FROM check_ins WHERE timestamp BETWEEN ? AND ?"
        params = (start_dt_utc.strftime('%Y-%m-%d %H:%M:%S'), end_dt_utc.strftime('%Y-%m-%d %H:%M:%S'))
        cursor_events = await db.execute(query, params)

        # 3. Распределяем сотрудников по категориям
        arrivals = {}
        departures = set()
        
        for emp_id, check_type, status in await cursor_events.fetchall():
            if emp_id not in scheduled_employees: continue # Пропускаем, если сотрудник не по графику

            # Обрабатываем отпуска и отгулы
            if check_type == 'SYSTEM_LEAVE':
                stats['on_leave'][emp_id] = {'name': scheduled_employees[emp_id], 'status': status}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
            # Обрабатываем приходы
            elif check_type == 'ARRIVAL' and status in ('SUCCESS', 'LATE'):
                arrivals[emp_id] = {'name': scheduled_employees[emp_id], 'status': status}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
            # Обрабатываем уходы
            elif check_type == 'DEPARTURE' and status == 'SUCCESS':
                departures.add(emp_id)
            # Обрабатываем штрафы за неотмеченный уход
            elif check_type == 'SYSTEM' and status == 'ABSENT_INCOMPLETE':
                stats['incomplete'][emp_id] = scheduled_employees[emp_id]
                if emp_id in stats['absent']: del stats['absent'][emp_id]

        # 4. Финальная сверка
        for emp_id, data in arrivals.items():
            if emp_id in stats['on_leave'] or emp_id in stats['incomplete']:
                continue # Отпуск или штраф имеют приоритет
            
            if emp_id in departures:
                stats['departed'][emp_id] = data['name']
            else:
                stats['arrived'][emp_id] = data
    
    return stats