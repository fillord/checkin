# database.py
import logging
import asyncpg
import numpy as np
import calendar
from datetime import datetime, date, time, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
from config import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, LOCAL_TIMEZONE

logger = logging.getLogger(__name__)

# --- Вспомогательная функция для создания комбинированного статуса ---
def _build_composite_status(status_list: list, is_work_day: bool, is_past_date: bool) -> str:
    """
    Создает детализированную строку статуса на основе списка всех событий за день.
    """
    if not is_work_day:
        return "Выходной"

    # Статусы, которые определяют весь день, имеют наивысший приоритет.
    if 'VACATION' in status_list:
        return 'Отпуск'
    if 'SICK_LEAVE' in status_list:
        return 'Больничный'

    # Определяем статус прихода
    arrival_status = ""
    if 'LATE' in status_list:
        arrival_status = "Опоздал"
    elif 'SUCCESS' in status_list:
        arrival_status = "Вовремя"
    
    # Если сотрудник не пришел на работу
    if not arrival_status:
        if is_past_date:
            return "Пропустил"  # День прошел, а сотрудника не было
        else:
            return "—"  # Будущий рабочий день

    # Определяем статус ухода или завершения дня
    departure_status = ""
    if 'APPROVED_LEAVE' in status_list:
        departure_status = "Отпросился"
    # Статус "не завершил день" добавляется, если не было явного ухода пораньше
    elif 'ABSENT_INCOMPLETE' in status_list:
        departure_status = "Не завершил день"

    # Собираем итоговую строку
    final_parts = [arrival_status]
    if departure_status:
        final_parts.append(departure_status)
        
    return ", ".join(final_parts)


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
        # Добавляем таблицу для отпусков/больничных, чтобы не смешивать с check_ins
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
        logger.info("База данных PostgreSQL инициализирована.")
    finally:
        await conn.close()

# ... (остальные функции до get_monthly_summary_data остаются без изменений) ...

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

async def get_all_active_employees(search_query: str = None, sort_by: str = 'full_name', sort_order: str = 'asc') -> list[dict]:
    """
    Получает список всех активных сотрудников из PostgreSQL с возможностью поиска и сортировки.
    """
    # Белый список колонок для сортировки, чтобы предотвратить SQL-инъекции
    allowed_sort_columns = ['telegram_id', 'full_name']
    if sort_by not in allowed_sort_columns:
        sort_by = 'full_name'  # Значение по умолчанию

    # Белый список для направления сортировки
    sort_order = 'DESC' if sort_order.lower() == 'desc' else 'ASC'

    query_params = []
    # Базовый запрос
    sql = "SELECT telegram_id, full_name, is_active FROM employees WHERE is_active = TRUE"

    # Если есть поисковый запрос, добавляем условие WHERE
    # ILIKE - это регистронезависимый поиск в PostgreSQL
    if search_query:
        sql += " AND full_name ILIKE $1"
        query_params.append(f"%{search_query}%")

    # Добавляем сортировку
    sql += f" ORDER BY {sort_by} {sort_order}"
    conn = await get_db_connection()
    try:
        # Выполняем запрос с динамически собранными параметрами
        rows = await conn.fetch(sql, *query_params)
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

async def is_day_finished_for_user(telegram_id: int) -> bool:
    """
    Проверяет, завершен ли рабочий день для сотрудника (был уход или системная отметка).
    Адаптировано для PostgreSQL.
    """
    today_local = datetime.now(LOCAL_TIMEZONE).date()
    start_of_day_utc = datetime.combine(today_local, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
    end_of_day_utc = datetime.combine(today_local, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
    
    conn = await get_db_connection()
    try:
        # Сначала проверяем отпуска/больничные
        leave_query = "SELECT 1 FROM leaves WHERE employee_telegram_id = $1 AND start_date <= $2 AND end_date >= $2 LIMIT 1"
        is_on_leave = await conn.fetchval(leave_query, telegram_id, today_local)
        if is_on_leave:
            return True

        # Затем проверяем чекины
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

async def add_leave_period(telegram_id: int, start_date: date, end_date: date, leave_type: str):
    """Добавляет записи об отпуске/больничном в отдельную таблицу 'leaves'."""
    conn = await get_db_connection()
    try:
        # Убедимся, что тип отпуска соответствует ожидаемым значениям
        leave_status = 'VACATION' if 'отпуск' in leave_type.lower() else 'SICK_LEAVE'
        
        # Просто вставляем одну запись на весь период. Это гораздо эффективнее.
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
        event_rows = await conn.fetch(
            "SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2", 
            start_dt_utc, end_dt_utc
        )
        
        # Группируем события по дате и сотруднику
        events_by_day = defaultdict(lambda: defaultdict(list))
        for row in event_rows:
            local_date = row['timestamp'].astimezone(LOCAL_TIMEZONE).date()
            events_by_day[local_date][row['employee_telegram_id']].append(row['status'])

        # Получаем все отпуска/больничные за период
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
        

        # Проверяем график для каждого дня индивидуально
        for current_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
            for emp_id, name in all_employees.items():
                schedule_for_day = await get_schedule_for_specific_date(conn, emp_id, current_date)
                
                if schedule_for_day: # Если день был рабочим
                    stats['total_work_days'] += 1
                    
                    # Проверяем отпуск/больничный
                    if leaves_by_day.get(current_date, {}).get(emp_id):
                        continue

                    day_events = events_by_day.get(current_date, {}).get(emp_id, [])
                    
                    if 'LATE' in day_events:
                        stats['total_arrivals'] += 1
                        stats['total_lates'] += 1
                        stats['late_employees'][name].append(current_date.strftime('%d.%m'))
                    elif 'SUCCESS' in day_events:
                        stats['total_arrivals'] += 1
                    else: # Не было прихода
                        if current_date < datetime.now(LOCAL_TIMEZONE).date():
                            stats['absences'][name].append(current_date.strftime('%d.%m'))

    finally:
        await conn.close()
    return stats

async def cancel_leave_period(telegram_id: int, start_date: date, end_date: date) -> int:
    """Удаляет записи об отпуске/больничном для сотрудника на заданный период в PostgreSQL."""
    conn = await get_db_connection()
    try:
        # Удаляем все периоды, которые ПЕРЕСЕКАЮТСЯ с заданным диапазоном
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

# --- ПОЛНОСТЬЮ ПЕРЕРАБОТАННАЯ ФУНКЦИЯ ---
async def get_monthly_summary_data(year: int, month: int) -> list[list]:
    """Собирает и формирует данные для сводного месячного отчета с КОМБИНИРОВАННЫМИ статусами."""
    conn = await get_db_connection()
    try:
        start_date = date(year, month, 1)
        num_days = calendar.monthrange(year, month)[1]
        end_date = date(year, month, num_days)
        today = datetime.now(LOCAL_TIMEZONE).date()
    except ValueError:
        logger.error(f"Неверный год или месяц: {year}-{month}")
        await conn.close()
        return []

    try:
        # 1. Получаем всех активных сотрудников
        emp_rows = await conn.fetch("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE ORDER BY full_name")
        all_employees = {row['telegram_id']: row['full_name'] for row in emp_rows}
        
        # 2. Получаем все события чекинов за месяц
        start_dt_utc = datetime.combine(start_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_dt_utc = datetime.combine(end_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        
        query = "SELECT employee_telegram_id, timestamp, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2"
        event_rows = await conn.fetch(query, start_dt_utc, end_dt_utc)

        # 3. Группируем все статусы по сотруднику и по дню
        checkins = defaultdict(lambda: defaultdict(list))
        for row in event_rows:
            local_date_iso = row['timestamp'].astimezone(LOCAL_TIMEZONE).date().isoformat()
            checkins[row['employee_telegram_id']][local_date_iso].append(row['status'])

        # 4. Получаем все отпуска и больничные, которые пересекаются с месяцем
        leaves_rows = await conn.fetch(
            "SELECT employee_telegram_id, start_date, end_date, leave_type FROM leaves WHERE start_date <= $1 AND end_date >= $2",
            end_date, start_date
        )
        # Добавляем статусы отпусков и больничных в общую структуру
        for row in leaves_rows:
            current_date = row['start_date']
            while current_date <= row['end_date']:
                if start_date <= current_date <= end_date:
                     checkins[row['employee_telegram_id']][current_date.isoformat()].append(row['leave_type'])
                current_date += timedelta(days=1)


        # 5. Формируем итоговую таблицу
        header = ["Сотрудник"] + [f"{day:02d}.{month:02d}" for day in range(1, num_days + 1)]
        result_table = [header]

        for emp_id, name in all_employees.items():
            employee_row = [name]
            for day in range(1, num_days + 1):
                current_date = date(year, month, day)
                
                # Получаем график на конкретный день
                schedule_for_day = await get_schedule_for_specific_date(conn, emp_id, current_date)
                
                # Получаем список всех статусов за этот день для сотрудника
                status_list = checkins.get(emp_id, {}).get(current_date.isoformat(), [])
                
                # Вызываем новую функцию для создания комбинированного статуса
                final_status_str = _build_composite_status(
                    status_list, 
                    is_work_day=(schedule_for_day is not None),
                    is_past_date=(current_date <= today)
                )
                employee_row.append(final_status_str)
                
            result_table.append(employee_row)

        return result_table
    finally:
        await conn.close()
# --- КОНЕЦ ПЕРЕРАБОТАННОЙ ФУНКЦИИ ---

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

        # 2. Получаем все события за сегодня (и чекины, и отпуска)
        start_of_day_utc = datetime.combine(for_date, time.min, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))
        end_of_day_utc = datetime.combine(for_date, time.max, tzinfo=LOCAL_TIMEZONE).astimezone(ZoneInfo("UTC"))

        # Достаем отпуска
        leaves_rows = await conn.fetch("SELECT employee_telegram_id, leave_type FROM leaves WHERE start_date <= $1 AND end_date >= $1", for_date)
        for row in leaves_rows:
            emp_id = row['employee_telegram_id']
            if emp_id in scheduled_employees:
                stats['on_leave'][emp_id] = {'name': scheduled_employees[emp_id], 'status': row['leave_type']}
                if emp_id in stats['absent']: del stats['absent'][emp_id]
        
        # Достаем чекины
        query_events = "SELECT employee_telegram_id, check_in_type, status FROM check_ins WHERE timestamp BETWEEN $1 AND $2"
        event_rows = await conn.fetch(query_events, start_of_day_utc, end_of_day_utc)

        arrivals = {}
        departures = set()
        
        for row in event_rows:
            emp_id = row['employee_telegram_id']
            check_type = row['check_in_type']
            status = row['status']
            
            if emp_id not in scheduled_employees: continue
            if emp_id in stats['on_leave']: continue # Если в отпуске, игнорируем чекины

            if check_type == 'SYSTEM_LEAVE' and status == 'APPROVED_LEAVE':
                 # Отпросился - это частный случай ухода, не полного отсутствия
                 departures.add(emp_id)
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
            if emp_id in stats['incomplete']:
                continue 
            
            if emp_id in departures:
                stats['departed'][emp_id] = data['name']
            else:
                stats['arrived'][emp_id] = data
    finally:
        await conn.close()
    
    return stats