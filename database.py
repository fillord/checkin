# database.py
import logging
import asyncpg
import numpy as np
import calendar
from datetime import datetime, date, time, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
from config import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, LOCAL_TIMEZONE

# Убираем кэширующий декоратор, так как он несовместим с новым способом передачи соединения
from async_lru import alru_cache 

logger = logging.getLogger(__name__)

# Строка подключения (DSN) для PostgreSQL
# dsn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

async def get_db_connection():
    """
    Устанавливает соединение с базой данных PostgreSQL,
    передавая параметры отдельно для безопасности.
    """
    return await asyncpg.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST
    )

async def init_db():
    """Инициализирует таблицы в базе данных PostgreSQL с правильными типами данных."""
    # Устанавливаем соединение
    conn = await get_db_connection()
    try:
        # Создаем таблицу employees
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                telegram_id BIGINT PRIMARY KEY,
                full_name TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                face_encoding BYTEA
            );
        """)
        
        # Создаем таблицу schedules
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
        
        # Создаем таблицу check_ins
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
        logger.info("База данных PostgreSQL инициализирована.")
    finally:
        # Всегда закрываем соединение
        await conn.close()

# database.py

async def get_employee_data(telegram_id: int, include_inactive=False) -> dict | None:
    """Получает данные сотрудника из PostgreSQL."""
    # 1. Формируем SQL-запрос с плейсхолдерами для PostgreSQL ($1)
    sql = "SELECT telegram_id, full_name, face_encoding, is_active FROM employees WHERE telegram_id = $1"
    if not include_inactive:
        sql += " AND is_active = TRUE"
    
    # 2. Получаем соединение
    conn = await get_db_connection()
    try:
        # 3. Выполняем запрос с помощью conn.fetchrow() для получения одной строки
        row = await conn.fetchrow(sql, telegram_id)
        # 4. Преобразуем результат (asyncpg.Record) в словарь, если он есть
        if row:
            return dict(row)
    finally:
        # 5. Гарантированно закрываем соединение
        await conn.close()
    return None

# database.py

async def get_all_active_employees() -> list[dict]:
    """Получает список всех активных сотрудников из PostgreSQL."""
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT telegram_id, full_name, is_active FROM employees WHERE is_active = TRUE ORDER BY full_name")
        return [dict(row) for row in rows]
    finally:
        await conn.close()

async def is_employee_active(telegram_id: int) -> bool:
    """Проверяет, активен ли сотрудник в базе данных PostgreSQL."""
    conn = await get_db_connection()
    try:
        # Используем conn.fetchval() для получения одного значения из одной строки - это очень эффективно
        is_active = await conn.fetchval(
            "SELECT is_active FROM employees WHERE telegram_id = $1",
            telegram_id
        )
        # fetchval вернет None, если ничего не найдено, или значение (True/False)
        return is_active if is_active is not None else False
    finally:
        await conn.close()


# ВНИМАНИЕ: Декоратор @alru_cache убран.
# Простое кэширование не работает с новой моделью соединений asyncpg, так как объект соединения
# будет меняться при каждом вызове. Мы можем реализовать более сложный кэш позже, если понадобится.
# database.py

async def get_all_active_employees_with_schedules(for_date: date) -> list:
    """
    Получает список активных сотрудников и их АКТУАЛЬНЫЙ на for_date график.
    Возвращает ТОЛЬКО тех, у кого сегодня рабочий день.
    """
    # Этот сложный запрос с оконной функцией эффективно получает последнюю версию графика для каждого сотрудника
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


###
# database.py

async def get_schedule_for_specific_date(conn: asyncpg.Connection, telegram_id: int, target_date: date) -> dict | None:
    """Получает корректную версию графика для сотрудника на КОНКРЕТНУЮ дату, используя переданное соединение."""
    query = """
        SELECT start_time, end_time FROM schedules
        WHERE employee_telegram_id = $1 AND day_of_week = $2 AND effective_from_date <= $3
        ORDER BY effective_from_date DESC LIMIT 1
    """
    row = await conn.fetchrow(query, telegram_id, target_date.weekday(), target_date)
    
    # asyncpg возвращает объекты datetime.time, поэтому fromisoformat не нужен
    if row and row['start_time'] is not None and row['end_time'] is not None:
        return {"start_time": row['start_time'], "end_time": row['end_time']}
    
    return None

# ВНИМАНИЕ: Декоратор @alru_cache убран. Мы можем вернуть кэширование позже, если потребуется.
async def get_employee_today_schedule(telegram_id: int) -> dict | None:
    """Получает актуальный график сотрудника на СЕГОДНЯ из PostgreSQL."""
    today = datetime.now(LOCAL_TIMEZONE).date()
    conn = await get_db_connection()
    try:
        emp_row = await conn.fetchrow("SELECT full_name FROM employees WHERE telegram_id = $1", telegram_id)
        if not emp_row: 
            return None
        
        # Передаем уже открытое соединение в нашу вспомогательную функцию
        schedule = await get_schedule_for_specific_date(conn, telegram_id, today)
        if schedule:
            schedule['name'] = emp_row['full_name']
            return schedule
    finally:
        await conn.close()
    return None

async def has_checked_in_on_date(telegram_id: int, check_in_type: str, for_date: date) -> bool:
    """Проверяет наличие чекина за конкретную дату в PostgreSQL."""
    start_of_day_local = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_of_day_local = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE)
    start_of_day_utc = start_of_day_local.astimezone(ZoneInfo("UTC"))
    end_of_day_utc = end_of_day_local.astimezone(ZoneInfo("UTC"))

    statuses_to_check = ('SUCCESS', 'LATE') if check_in_type == 'ARRIVAL' else ('SUCCESS',)

    conn = await get_db_connection()
    try:
        # Используем конструкцию `status = ANY($3)` - это безопасный способ для PostgreSQL
        # передать список значений, в отличие от форматирования строки.
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
    """Проверяет наличие чекина за СЕГОДНЯ, используя более общую функцию."""
    # Чтобы не дублировать код, просто вызываем нашу новую функцию с сегодняшней датой
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    return await has_checked_in_on_date(telegram_id, check_in_type, today_local)


####
# database.py

async def is_day_finished_for_user(telegram_id: int) -> bool:
    """
    Проверяет, завершен ли рабочий день для сотрудника (был уход или системная отметка).
    Адаптировано для PostgreSQL.
    """
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    start_of_day_utc = datetime.combine(today_local, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
    end_of_day_utc = datetime.combine(today_local, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
    
    statuses_to_check = ('SUCCESS', 'APPROVED_LEAVE', 'VACATION', 'SICK_LEAVE')
    
    conn = await get_db_connection()
    try:
        # Используем конструкцию `status = ANY($3)` для безопасной передачи списка статусов
        query = """
            SELECT 1 FROM check_ins 
            WHERE employee_telegram_id = $1
              AND (check_in_type = 'DEPARTURE' OR check_in_type = 'SYSTEM_LEAVE')
              AND status = ANY($2)
              AND timestamp BETWEEN $3 AND $4
            LIMIT 1
        """
        # Передаем параметры в правильном порядке для $1, $2, $3, $4
        row = await conn.fetchrow(query, telegram_id, list(statuses_to_check), start_of_day_utc, end_of_day_utc)
        return row is not None
    finally:
        await conn.close()

async def set_employee_active_status(telegram_id: int, is_active: bool):
    """Устанавливает статус активности сотрудника в PostgreSQL."""
    conn = await get_db_connection()
    try:
        # conn.execute достаточно для INSERT/UPDATE/DELETE, commit не нужен
        await conn.execute(
            "UPDATE employees SET is_active = $1 WHERE telegram_id = $2",
            is_active, telegram_id
        )
    finally:
        await conn.close()

async def set_face_encoding(telegram_id: int, encoding: np.ndarray):
    """Сохраняет кодировку лица (как BYTEA) в PostgreSQL."""
    encoding_bytes = encoding.tobytes()
    conn = await get_db_connection()
    try:
        await conn.execute(
            "UPDATE employees SET face_encoding = $1 WHERE telegram_id = $2",
            encoding_bytes, telegram_id
        )
    finally:
        await conn.close()
# database.py

async def add_or_update_employee(telegram_id: int, full_name: str, schedule_data: dict, effective_date: date):
    """
    Добавляет/обновляет сотрудника и его график, используя надежный механизм
    PostgreSQL INSERT ... ON CONFLICT. Этот метод гарантирует перезапись.
    """
    conn = await get_db_connection()
    try:
        # Используем транзакцию, чтобы все 7 дней обновились как единое целое.
        async with conn.transaction():
            # Шаг 1: Обновляем самого сотрудника
            await conn.execute(
                """
                INSERT INTO employees (telegram_id, full_name, is_active) VALUES ($1, $2, TRUE)
                ON CONFLICT (telegram_id) DO UPDATE SET full_name = $2, is_active = TRUE
                """,
                telegram_id, full_name
            )

            # Шаг 2: Обновляем/вставляем график для каждого из 7 дней
            for day_of_week in range(7):
                times = schedule_data.get(day_of_week)
                
                # Получаем время (объект time) или None для выходного
                start_time = times.get('start') if times else None
                end_time = times.get('end') if times else None
                
                # Используем ON CONFLICT для атомарного обновления или вставки.
                # Если запись с таким (id, день, дата) уже есть, она будет обновлена.
                # Если нет - будет вставлена новая.
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
    """Логирует попытку чекина в PostgreSQL."""
    conn = await get_db_connection()
    try:
        # Мы не передаем timestamp, так как в таблице стоит DEFAULT NOW() AT TIME ZONE 'utc'
        # База данных сама подставит корректное UTC время.
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
    """Вставляет в PostgreSQL системную запись о прогуле из-за отсутствия чекина ухода."""
    # Создаем datetime объект в локальной таймзоне
    end_of_day_local = datetime.combine(for_date, time(23, 59, 59), tzinfo=LOCAL_TIMEZONE)
    # Конвертируем в UTC. asyncpg сам обработает объект datetime.
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

# database.py

async def add_leave_period(telegram_id: int, start_date: date, end_date: date, leave_type: str):
    """
    Добавляет записи об отпуске/больничном для сотрудника на заданный период,
    покрывая ВСЕ дни в периоде, включая выходные.
    """
    conn = await get_db_connection()
    try:
        leave_status = 'VACATION' if leave_type == 'Отпуск' else 'SICK_LEAVE'
        
        # Используем транзакцию, чтобы все записи добавились как единое целое
        async with conn.transaction():
            # Проходим по каждому дню в указанном пользователем диапазоне
            for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
                
                # Определяем начало и конец этого дня в UTC для точного поиска
                start_of_day_utc = datetime.combine(current_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
                end_of_day_utc = datetime.combine(current_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))

                # Сначала удаляем любые предыдущие отметки об отпуске/больничном за этот день, чтобы избежать дублей
                await conn.execute(
                    "DELETE FROM check_ins WHERE employee_telegram_id = $1 AND check_in_type = 'SYSTEM_LEAVE' AND timestamp BETWEEN $2 AND $3",
                    telegram_id, start_of_day_utc, end_of_day_utc
                )
                
                # Теперь вставляем новую, правильную отметку в конец дня
                timestamp_to_insert = end_of_day_utc
                await conn.execute(
                    "INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status) VALUES ($1, $2, $3, $4)",
                    timestamp_to_insert, telegram_id, 'SYSTEM_LEAVE', leave_status
                )

        logger.info(f"Для сотрудника {telegram_id} назначен(а) {leave_type} с {start_date} по {end_date} (включая выходные).")
    finally:
        await conn.close()


async def get_all_checkins_for_export() -> list:
    """Получает все записи для CSV-экспорта из PostgreSQL."""
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
        # Возвращаем список кортежей для совместимости с модулем csv
        return [tuple(row.values()) for row in rows]
    finally:
        await conn.close()


async def get_report_stats_for_period(start_date: date, end_date: date) -> dict:
    """Собирает статистику для текстового отчета из PostgreSQL."""
    stats = {
        'total_work_days': 0, 'total_arrivals': 0, 'total_lates': 0,
        'absences': defaultdict(list), 'late_employees': defaultdict(list)
    }
    conn = await get_db_connection()
    try:
        start_dt_utc = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_dt_utc = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        
        emp_rows = await conn.fetch("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE")
        all_employees = {row['telegram_id']: row['full_name'] for row in emp_rows}
        
        # Получаем все события за период
        event_rows = await conn.fetch("SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2", start_dt_utc, end_dt_utc)
        
        events_by_date = defaultdict(dict)
        for row in event_rows:
            local_date = row['timestamp'].astimezone(LOCAL_TIMEZONE).date()
            # Устанавливаем статус, отдавая приоритет системным отметкам о прогуле
            if row['status'] == 'ABSENT_INCOMPLETE':
                events_by_date[local_date][row['employee_telegram_id']] = 'ABSENT_INCOMPLETE'
            elif row['employee_telegram_id'] not in events_by_date.get(local_date, {}):
                events_by_date[local_date][row['employee_telegram_id']] = row['status']

        # Проверяем график для каждого дня индивидуально
        for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
            for emp_id, name in all_employees.items():
                schedule_for_day = await get_schedule_for_specific_date(conn, emp_id, current_date)
                
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
    finally:
        await conn.close()
    return stats

# database.py

async def cancel_leave_period(telegram_id: int, start_date: date, end_date: date) -> int:
    """Удаляет записи об отпуске/больничном для сотрудника на заданный период в PostgreSQL."""
    conn = await get_db_connection()
    try:
        start_dt_utc = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_dt_utc = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))

        leave_statuses = ('VACATION', 'SICK_LEAVE', 'APPROVED_LEAVE')
        
        query = f"""
            DELETE FROM check_ins 
            WHERE employee_telegram_id = $1 
            AND status = ANY($2)
            AND timestamp BETWEEN $3 AND $4
        """
        # asyncpg.execute возвращает строку статуса, например "DELETE 5"
        status_str = await conn.execute(query, telegram_id, list(leave_statuses), start_dt_utc, end_dt_utc)
        
        # Извлекаем количество удаленных строк из статуса
        rows_deleted = int(status_str.split()[-1])
        
        logger.info(f"Для сотрудника {telegram_id} отменено отсутствие с {start_date} по {end_date}. Удалено записей: {rows_deleted}")
        return rows_deleted
    finally:
        await conn.close()


async def get_monthly_summary_data(year: int, month: int) -> list[list]:
    """Собирает и формирует данные для сводного месячного отчета из PostgreSQL."""
    conn = await get_db_connection()
    try:
        start_date = date(year, month, 1)
        num_days = calendar.monthrange(year, month)[1]
        end_date = date(year, month, num_days)
    except ValueError:
        logger.error(f"Неверный год или месяц: {year}-{month}")
        await conn.close()
        return []

    try:
        all_employees = {}
        checkins = defaultdict(dict)

        emp_rows = await conn.fetch("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE ORDER BY full_name")
        all_employees = {row['telegram_id']: row['full_name'] for row in emp_rows}
        
        start_dt_utc = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_dt_utc = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        
        query = """
            SELECT employee_telegram_id, timestamp, status FROM check_ins 
            WHERE timestamp BETWEEN $1 AND $2 
            ORDER BY CASE status 
                WHEN 'ABSENT_INCOMPLETE' THEN 1 
                WHEN 'VACATION' THEN 2
                WHEN 'SICK_LEAVE' THEN 2
                WHEN 'APPROVED_LEAVE' THEN 2
                ELSE 3 
            END
        """
        event_rows = await conn.fetch(query, start_dt_utc, end_dt_utc)
        for row in event_rows:
            local_date_iso = row['timestamp'].astimezone(LOCAL_TIMEZONE).date().isoformat()
            if row['employee_telegram_id'] not in checkins or local_date_iso not in checkins[row['employee_telegram_id']]:
                 checkins[row['employee_telegram_id']][local_date_iso] = row['status']

        header = ["Сотрудник"] + [f"{day:02d}.{month:02d}" for day in range(1, num_days + 1)]
        result_table = [header]

        for emp_id, name in all_employees.items():
            employee_row = [name]
            for day in range(1, num_days + 1):
                current_date = date(year, month, day)
                # Передаем соединение conn в нашу вспомогательную функцию
                schedule_for_day = await get_schedule_for_specific_date(conn, emp_id, current_date)
                
                status_str = "Выходной"
                if schedule_for_day:
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
    finally:
        await conn.close()

# database.py

async def get_dashboard_stats(for_date: date) -> dict:
    """Собирает оперативную статистику за указанную дату для дашборда из PostgreSQL."""
    stats = {
        'total_scheduled': 0,
        'arrived': {},      # {id: {'name': name, 'status': 'LATE'/'SUCCESS'}}
        'departed': {},     # {id: name}
        'on_leave': {},     # {id: {'name': name, 'status': 'VACATION'/'SICK_LEAVE'/...}}
        'absent': {},       # {id: name}
        'incomplete': {}    # {id: name}
    }
    
    conn = await get_db_connection()
    try:
        # 1. Получаем всех, кто должен работать сегодня, с учетом ВЕРСИИ графика
        # Этот запрос гарантирует, что мы берем только тех, у кого на for_date действительно рабочий день
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
        
        # По умолчанию все, кто должен работать, - прогульщики
        stats['absent'] = scheduled_employees.copy()

        # 2. Получаем все события за сегодня
        start_of_day_utc = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_of_day_utc = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))

        query_events = "SELECT employee_telegram_id, check_in_type, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2"
        event_rows = await conn.fetch(query_events, start_of_day_utc, end_of_day_utc)

        # 3. Распределяем сотрудников по категориям
        arrivals = {}
        departures = set()
        
        for row in event_rows:
            emp_id = row['employee_telegram_id']
            check_type = row['check_in_type']
            status = row['status']
            
            if emp_id not in scheduled_employees: continue

            if check_type == 'SYSTEM_LEAVE':
                stats['on_leave'][emp_id] = {'name': scheduled_employees[emp_id], 'status': status}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
            elif check_type == 'ARRIVAL' and status in ('SUCCESS', 'LATE'):
                arrivals[emp_id] = {'name': scheduled_employees[emp_id], 'status': status}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
            elif check_type == 'DEPARTURE' and status == 'SUCCESS':
                departures.add(emp_id)
            elif check_type == 'SYSTEM' and status == 'ABSENT_INCOMPLETE':
                stats['incomplete'][emp_id] = scheduled_employees[emp_id]
                if emp_id in stats['absent']: del stats['absent'][emp_id]

        # 4. Финальная сверка приходов и уходов
        for emp_id, data in arrivals.items():
            if emp_id in stats['on_leave'] or emp_id in stats['incomplete']:
                continue 
            
            if emp_id in departures:
                stats['departed'][emp_id] = data['name']
            else:
                stats['arrived'][emp_id] = data
    finally:
        await conn.close()
    
    return stats