# -*- coding: utf-8 -*-

"""
Telegram Check-in Bot v12.9 (Final Full Code - Refactored)
===========================================================

Это финальная, полная версия бота со всеми функциями, включая:
- Панель администратора (/admin)
- Добавление, изменение и удаление сотрудников
- Расширенные отчеты (за сегодня, вчера, неделю, период)
- Экспорт всех данных в CSV
- Проверка на "живость" при чекине
- Уведомления об опозданиях
- Сохранение состояния диалогов после перезапуска

Исправлена ошибка с повторными чек-инами из-за часовых поясов.

Перед запуском:
1.  Убедитесь, что все зависимости установлены.
    pip install "python-telegram-bot>=21.0" aiosqlite geopy python-dotenv face_recognition numpy opencv-python apscheduler
2.  Заполните файл .env, указав ваш TELEGRAM_BOT_TOKEN.
3.  Заполните список ADMIN_IDS вашим Telegram ID.
4.  ВАЖНО: Полностью удалите старый файл базы данных и файл состояний (bot_persistence.pickle).
5.  Запустите скрипт.
"""

import logging
import asyncio
import aiosqlite
import os
import re
import random
import csv
from datetime import time, datetime, date, timedelta
from dotenv import load_dotenv
from io import StringIO, BytesIO
import numpy as np
from collections import defaultdict

# Библиотеки для распознавания лиц и планировщика
import face_recognition
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, MessageOriginUser, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    PicklePersistence,
    CallbackQueryHandler
)
from geopy.distance import geodesic

# --- НАСТРОЙКИ ---

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Не найден TELEGRAM_BOT_TOKEN в файле .env.")

ADMIN_IDS = [1027958463]  # !!! ЗАМЕНИТЕ НА РЕАЛЬНЫЕ ID АДМИНОВ !!!

from zoneinfo import ZoneInfo
LOCAL_TIMEZONE = ZoneInfo("Asia/Almaty")

WORK_LOCATION_COORDS = (43.26103183044612, 76.89106713108873)
ALLOWED_RADIUS_METERS = 200
FACE_DISTANCE_THRESHOLD = 0.6 

DB_NAME = "checkin_bot_final.db"
PERSISTENCE_FILE = "bot_persistence.pickle"

# --- Состояния для диалогов ---
CHOOSE_ACTION, AWAITING_PHOTO, AWAITING_LOCATION, REGISTER_FACE = range(4)
(
    ADMIN_MENU, ADMIN_REPORTS_MENU,
    ADD_GET_ID, ADD_GET_NAME,
    MODIFY_GET_ID,
    DELETE_GET_ID, DELETE_CONFIRM,
    SCHEDULE_MON, SCHEDULE_TUE, SCHEDULE_WED, SCHEDULE_THU, SCHEDULE_FRI, SCHEDULE_SAT, SCHEDULE_SUN,
    REPORT_GET_DATES
) = range(4, 19)

# --- Кнопки ---
BUTTON_ARRIVAL = "✅ Приход"
BUTTON_DEPARTURE = "🏁 Уход"
BUTTON_ADMIN_ADD = "➕ Добавить сотрудника"
BUTTON_ADMIN_MODIFY = "✏️ Изменить график"
BUTTON_ADMIN_DELETE = "❌ Удалить сотрудника"
BUTTON_ADMIN_REPORTS = "📊 Отчеты"
BUTTON_ADMIN_BACK = "◀️ Назад в меню"
BUTTON_REPORT_TODAY = "🗓️ За сегодня"
BUTTON_REPORT_YESTERDAY = "⏪ За вчера"
BUTTON_REPORT_WEEK = "📅 За неделю"
BUTTON_REPORT_CUSTOM = "🔎 Отчет за период"
BUTTON_REPORT_EXPORT = "📄 Экспорт в CSV"
BUTTON_CONFIRM_DELETE = "Да, удалить"
BUTTON_CANCEL_DELETE = "Нет, отмена"

LIVENESS_ACTIONS = ["улыбнитесь в камеру", "покажите на камеру большой палец 👍", "покажите на камеру знак 'мир' двумя пальцами ✌️"]

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DAYS_OF_WEEK = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И ЛОГИКА ---

def parse_day_schedule(text: str) -> dict | None:
    text = text.strip().lower()
    if text in ("0", "выходной"): return {}
    time_pattern = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$")
    match = time_pattern.match(text)
    if match: return {"start": f"{match.group(1)}:{match.group(2)}", "end": f"{match.group(3)}:{match.group(4)}"}
    return None

def main_menu_keyboard(): 
    return ReplyKeyboardMarkup([[BUTTON_ARRIVAL, BUTTON_DEPARTURE]], resize_keyboard=True)

def admin_menu_keyboard(): 
    return ReplyKeyboardMarkup([[BUTTON_ADMIN_ADD], [BUTTON_ADMIN_MODIFY, BUTTON_ADMIN_DELETE], [BUTTON_ADMIN_REPORTS]], resize_keyboard=True)

def reports_menu_keyboard():
    return ReplyKeyboardMarkup([
        [BUTTON_REPORT_TODAY, BUTTON_REPORT_YESTERDAY], 
        [BUTTON_REPORT_WEEK, BUTTON_REPORT_CUSTOM], 
        [BUTTON_REPORT_EXPORT],
        [BUTTON_ADMIN_BACK]
    ], resize_keyboard=True)

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
        try:
            cursor = await db.execute("PRAGMA table_info(employees);")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'face_encoding' not in columns: await db.execute("ALTER TABLE employees ADD COLUMN face_encoding BLOB;")
            if 'is_active' not in columns: await db.execute("ALTER TABLE employees ADD COLUMN is_active BOOLEAN DEFAULT TRUE;")
        except aiosqlite.OperationalError as e: logger.error(f"Ошибка при модификации таблицы: {e}")
        
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
    sql = "SELECT telegram_id, full_name, face_encoding, is_active FROM employees WHERE telegram_id = ?"
    if not include_inactive:
        sql += " AND is_active = TRUE"
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(sql, (telegram_id,))
        row = await cursor.fetchone()
        if row:
            return {"id": row[0], "name": row[1], "face_encoding": row[2], "is_active": row[3] == 1}
    return None

async def get_all_active_employees_with_schedules(for_day: int) -> list:
    async with aiosqlite.connect(DB_NAME) as db:
        query = """
            SELECT e.telegram_id, e.full_name, s.start_time
            FROM employees e
            JOIN schedules s ON e.telegram_id = s.employee_telegram_id
            WHERE e.is_active = TRUE AND s.day_of_week = ?
        """
        cursor = await db.execute(query, (for_day,))
        return await cursor.fetchall()

async def get_employee_today_schedule(telegram_id: int) -> dict | None:
    today_weekday = datetime.now(LOCAL_TIMEZONE).weekday()
    async with aiosqlite.connect(DB_NAME) as db:
        query = "SELECT e.full_name, s.start_time, s.end_time FROM employees e JOIN schedules s ON e.telegram_id = s.employee_telegram_id WHERE e.telegram_id = ? AND s.day_of_week = ? AND e.is_active = TRUE"
        cursor = await db.execute(query, (telegram_id, today_weekday))
        row = await cursor.fetchone()
        if row: return {"name": row[0], "start_time": time.fromisoformat(row[1]), "end_time": time.fromisoformat(row[2])}
    return None

async def has_checked_in_today(telegram_id: int, check_in_type: str) -> bool:
    """ИСПРАВЛЕНИЕ: Проверяет наличие чекина за текущую локальную дату, используя `localtime`."""
    today_str = datetime.now(LOCAL_TIMEZONE).date().isoformat()
    statuses_to_check = ('SUCCESS', 'LATE') if check_in_type == 'ARRIVAL' else ('SUCCESS',)
    
    async with aiosqlite.connect(DB_NAME) as db:
        query = """
            SELECT 1 FROM check_ins 
            WHERE employee_telegram_id = ? 
              AND check_in_type = ? 
              AND status IN ({seq})
              AND DATE(timestamp, 'localtime') = ? 
            LIMIT 1
        """.format(seq=','.join('?' for _ in statuses_to_check))
        
        params = (telegram_id, check_in_type, *statuses_to_check, today_str)
        cursor = await db.execute(query, params)
        return await cursor.fetchone() is not None
        
async def set_employee_active_status(telegram_id: int, is_active: bool):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE employees SET is_active = ? WHERE telegram_id = ?", (is_active, telegram_id))
        await db.commit()

async def set_face_encoding(telegram_id: int, encoding: np.ndarray):
    encoding_bytes = encoding.tobytes()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE employees SET face_encoding = ? WHERE telegram_id = ?", (encoding_bytes, telegram_id))
        await db.commit()

async def add_or_update_employee(telegram_id: int, full_name: str, schedule_data: dict):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO employees (telegram_id, full_name, is_active) VALUES (?, ?, TRUE)", (telegram_id, full_name))
        await db.execute("UPDATE employees SET full_name = ?, is_active = TRUE WHERE telegram_id = ?", (full_name, telegram_id))
        await db.execute("DELETE FROM schedules WHERE employee_telegram_id = ?", (telegram_id,))
        for day_of_week, times in schedule_data.items():
            if times:
                await db.execute("INSERT INTO schedules (employee_telegram_id, day_of_week, start_time, end_time) VALUES (?, ?, ?, ?)", (telegram_id, day_of_week, times['start'], times['end']))
        await db.commit()

async def log_check_in_attempt(telegram_id: int, check_in_type: str, status: str, lat=None, lon=None, distance=None, similarity=None):
    async with aiosqlite.connect(DB_NAME) as db:
        # Сохраняем время в UTC, как и раньше
        timestamp_utc = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        await db.execute("INSERT INTO check_ins (timestamp, employee_telegram_id, check_in_type, status, latitude, longitude, distance_meters, face_similarity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                         (timestamp_utc, telegram_id, check_in_type, status, lat, lon, distance, similarity))
        await db.commit()


# --- НОВЫЕ ФУНКЦИИ ДЛЯ ОТЧЕТОВ ---
async def get_report_stats_for_period(start_date: date, end_date: date) -> dict:
    stats = {'total_work_days': 0, 'total_arrivals': 0, 'total_lates': 0, 'absences': defaultdict(list)}
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor_employees = await db.execute("SELECT telegram_id, full_name FROM employees WHERE is_active = TRUE")
        all_employees = {row[0]: row[1] for row in await cursor_employees.fetchall()}

        cursor_schedules = await db.execute("SELECT employee_telegram_id, day_of_week FROM schedules")
        schedules = defaultdict(set)
        for emp_id, day in await cursor_schedules.fetchall():
            schedules[emp_id].add(day)

        cursor_arrivals = await db.execute("SELECT employee_telegram_id, DATE(timestamp, 'localtime'), status FROM check_ins WHERE check_in_type = 'ARRIVAL' AND DATE(timestamp, 'localtime') BETWEEN ? AND ?", (start_date.isoformat(), end_date.isoformat()))
        arrivals_by_date = defaultdict(dict)
        for emp_id, date_str, status in await cursor_arrivals.fetchall():
            arrivals_by_date[date_str][emp_id] = status

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
                    else:
                        stats['absences'][name].append(current_date.strftime('%d.%m'))
    return stats

async def get_all_checkins_for_export() -> list:
    async with aiosqlite.connect(DB_NAME) as db:
        query = "SELECT c.timestamp, e.full_name, c.check_in_type, c.status, c.latitude, c.longitude, c.distance_meters, c.face_similarity FROM check_ins c JOIN employees e ON c.employee_telegram_id = e.telegram_id ORDER BY c.timestamp DESC"
        return await (await db.execute(query)).fetchall()

async def send_report_for_period(start_date: date, end_date: date, context: ContextTypes.DEFAULT_TYPE, title_prefix: str, chat_ids: list[int] | int):
    if not isinstance(chat_ids, list):
        chat_ids = [chat_ids]
    
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"Формирую отчет: {title_prefix}...")
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление о начале формирования отчета на {chat_id}: {e}")

    stats = await get_report_stats_for_period(start_date, end_date)
    
    start_str = start_date.strftime('%d\\.%m\\.%Y')
    end_str = end_date.strftime('%d\\.%m\\.%Y')
    period_str = start_str if start_date == end_date else f"с {start_str} по {end_str}"
    
    report_text = (
        f"📊 *{title_prefix} за {period_str}*\n\n"
        f"👥 *Всего рабочих дней \\(план\\):* {stats['total_work_days']}\n"
        f"✅ *Всего приходов \\(факт\\):* {stats['total_arrivals']}\n"
        f"🕒 *Из них опозданий:* {stats['total_lates']}\n\n"
        f"❌ *Пропуски ({len(stats['absences'])} человек\\(а\\)):*\n"
    )
    if stats['absences']:
        for name, dates in stats['absences'].items():
            report_text += f" \\- *{re.sub(r'([_*[\]()~`>#+\\-=|{}.!])', r'\\\1', name)}*: {', '.join(dates)}\n"
    else:
        report_text += " \\- Пропусков нет\\!\n"
        
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=chat_id, text=report_text, parse_mode='MarkdownV2')
        except Exception as e:
            logger.error(f"Не удалось отправить отчет на {chat_id}: {e}")

# --- УВЕДОМЛЕНИЯ И ЗАДАЧИ ---
async def send_daily_report_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Формирование и отправка автоматического дневного отчета...")
    await send_report_for_period(datetime.now(LOCAL_TIMEZONE).date(), datetime.now(LOCAL_TIMEZONE).date(), context, "Ежедневный отчет", ADMIN_IDS)

async def check_and_send_notifications(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(LOCAL_TIMEZONE)
    today_str = now.date().isoformat()
    if 'notifications_sent' not in context.bot_data: context.bot_data['notifications_sent'] = {}
    if context.bot_data.get('last_cleanup_date') != today_str:
        context.bot_data['notifications_sent'] = {}
        context.bot_data['last_cleanup_date'] = today_str

    employees = await get_all_active_employees_with_schedules(now.weekday())
    for emp_id, _, start_time_str in employees:
        start_time = time.fromisoformat(start_time_str)
        
        warning_time = (datetime.combine(now.date(), start_time) - timedelta(minutes=5)).time()
        warning_key = f"{emp_id}_warning"
        
        if now.time() >= warning_time and not context.bot_data['notifications_sent'].get(warning_key):
            if not await has_checked_in_today(emp_id, "ARRIVAL"):
                try:
                    await context.bot.send_message(chat_id=emp_id, text=f"🔔 Напоминание: ваш рабочий день скоро начнется. Пожалуйста, не забудьте отметиться.")
                    logger.info(f"Отправлено предупреждение сотруднику {emp_id}")
                except Exception as e: logger.error(f"Не удалось отправить предупреждение {emp_id}: {e}")
            context.bot_data['notifications_sent'][warning_key] = True

        missed_time = (datetime.combine(now.date(), start_time) + timedelta(minutes=6)).time()
        missed_key = f"{emp_id}_missed"
        if now.time() >= missed_time and not context.bot_data['notifications_sent'].get(missed_key):
            if not await has_checked_in_today(emp_id, "ARRIVAL"):
                keyboard = [[InlineKeyboardButton("Отметиться с опозданием", callback_data="late_checkin")]]
                try:
                    await context.bot.send_message(chat_id=emp_id, text="Вы пропустили время для чек-ина. Вы можете отметиться сейчас, но это будет зафиксировано как опоздание.", reply_markup=InlineKeyboardMarkup(keyboard))
                    logger.info(f"Отправлено уведомление об опоздании сотруднику {emp_id}")
                except Exception as e: logger.error(f"Не удалось отправить уведомление об опоздании {emp_id}: {e}")
            context.bot_data['notifications_sent'][missed_key] = True

async def late_checkin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    context.user_data["is_late"] = True
    await query.edit_message_text(text=f"Вы начали процесс чек-ина с опозданием.\n\nПожалуйста, {action} и сделайте селфи.")
    return AWAITING_PHOTO
    
async def verify_face(user_id: int, new_photo_file_id: str, context: ContextTypes.DEFAULT_TYPE) -> tuple[float, bool]:
    employee_data = await get_employee_data(user_id)
    if not employee_data or not employee_data["face_encoding"]: return 0.0, False
    known_encoding = np.frombuffer(employee_data["face_encoding"])
    try:
        new_photo_file = await context.bot.get_file(new_photo_file_id)
        photo_stream = BytesIO()
        await new_photo_file.download_to_memory(photo_stream)
        photo_stream.seek(0)
        image = face_recognition.load_image_file(photo_stream)
        def blocking_io_task():
            new_face_encodings = face_recognition.face_encodings(image)
            if not new_face_encodings: return 0.0, False
            distance = face_recognition.face_distance([known_encoding], new_face_encodings[0])[0]
            return max(0.0, (1.0 - distance) * 100), distance < FACE_DISTANCE_THRESHOLD
        similarity_score, is_match = await asyncio.to_thread(blocking_io_task)
        logger.info(f"Сравнение для {user_id}: схожесть {similarity_score:.2f}%. Результат: {is_match}")
        return similarity_score, is_match
    except Exception as e:
        logger.error(f"Ошибка во время верификации для {user_id}: {e}")
        return 0.0, False

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    employee_data = await get_employee_data(user.id)
    if not employee_data:
        await update.message.reply_text("Вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    if not employee_data["face_encoding"]:
        await update.message.reply_text(f"Здравствуйте, {employee_data['name']}!\n\nНужно зарегистрировать ваше лицо.", reply_markup=ReplyKeyboardRemove())
        return REGISTER_FACE
    await update.message.reply_text(f"Здравствуйте, {employee_data['name']}! Выберите действие:", reply_markup=main_menu_keyboard())
    return CHOOSE_ACTION

async def register_face(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    photo_file_id = update.message.photo[-1].file_id
    await update.message.reply_text("Спасибо. Обрабатываю фото...")
    try:
        photo_file = await context.bot.get_file(photo_file_id)
        photo_stream = BytesIO()
        await photo_file.download_to_memory(photo_stream)
        image = face_recognition.load_image_file(photo_stream)
        def blocking_io_task():
            return face_recognition.face_encodings(image)[0] if face_recognition.face_encodings(image) else None
        encoding = await asyncio.to_thread(blocking_io_task)
        if encoding is None:
            await update.message.reply_text("Лицо не найдено. Попробуйте другое фото.")
            return REGISTER_FACE
        await set_face_encoding(user.id, encoding)
        logger.info(f"Сотрудник {user.id} зарегистрировал эталонное лицо.")
        await update.message.reply_text("Отлично! Регистрация завершена.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    except Exception as e:
        logger.error(f"Ошибка регистрации лица для {user.id}: {e}")
        await update.message.reply_text("Произошла ошибка. Попробуйте еще раз.")
        return REGISTER_FACE

async def handle_arrival(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    schedule = await get_employee_today_schedule(user.id)
    if not schedule: return CHOOSE_ACTION
    if await has_checked_in_today(user.id, "ARRIVAL"):
        await update.message.reply_text("Вы уже отмечали приход сегодня.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    grace_period_end = (datetime.combine(date.today(), schedule['start_time']) + timedelta(minutes=5)).time()
    if datetime.now(LOCAL_TIMEZONE).time() > grace_period_end:
        await update.message.reply_text(f"Вы опоздали. Допустимое время для чекина было до {grace_period_end.strftime('%H:%M')}.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    await update.message.reply_text(f"Для подтверждения прихода, пожалуйста, {action} и сделайте селфи.", reply_markup=ReplyKeyboardRemove())
    return AWAITING_PHOTO

async def handle_departure(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    schedule = await get_employee_today_schedule(user.id)
    if not schedule: return CHOOSE_ACTION
    if not await has_checked_in_today(user.id, "ARRIVAL"):
        await update.message.reply_text("Вы не можете отметить уход, так как еще не отметили приход сегодня.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    if await has_checked_in_today(user.id, "DEPARTURE"):
        await update.message.reply_text("Вы уже отмечали уход сегодня.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    allowed_departure_start = (datetime.combine(date.today(), schedule['end_time']) - timedelta(minutes=10)).time()
    if datetime.now(LOCAL_TIMEZONE).time() < allowed_departure_start:
        await update.message.reply_text(f"Еще слишком рано для ухода. Вы можете отметиться после {allowed_departure_start.strftime('%H:%M')}.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "DEPARTURE"
    await update.message.reply_text(f"Для подтверждения ухода, пожалуйста, {action} и сделайте селфи.", reply_markup=ReplyKeyboardRemove())
    return AWAITING_PHOTO

async def awaiting_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['photo_file_id'] = update.message.photo[-1].file_id
    location_keyboard = [[KeyboardButton("Отправить мою геолокацию 📍", request_location=True)]]
    await update.message.reply_text("Отлично, фото получил. Теперь, пожалуйста, подтвердите вашу геолокацию.", reply_markup=ReplyKeyboardMarkup(location_keyboard, resize_keyboard=True, one_time_keyboard=True))
    return AWAITING_LOCATION

async def awaiting_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user, user_location = update.effective_user, update.message.location
    photo_file_id, check_in_type, is_late = context.user_data.get('photo_file_id'), context.user_data.get('checkin_type'), context.user_data.get('is_late', False)
    if not all([photo_file_id, check_in_type]):
        await update.message.reply_text("Что-то пошло не так. Начните заново с /start.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    await update.message.reply_text("Геолокация получена. Начинаю проверку...", reply_markup=ReplyKeyboardRemove())
    distance = round(geodesic(WORK_LOCATION_COORDS, (user_location.latitude, user_location.longitude)).meters, 2)
    if distance > ALLOWED_RADIUS_METERS:
        await log_check_in_attempt(user.id, check_in_type, 'FAIL_LOCATION', user_location.latitude, user_location.longitude, distance)
        await update.message.reply_text(f"❌ Чек-ин отклонен.\nВы находитесь слишком далеко от рабочего места ({distance} м).", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    face_similarity, is_match = await verify_face(user.id, photo_file_id, context)
    if not is_match:
        await log_check_in_attempt(user.id, check_in_type, 'FAIL_FACE', user_location.latitude, user_location.longitude, distance, face_similarity)
        await update.message.reply_text(f"❌ Чек-ин отклонен.\nЛицо на фото не распознано (схожесть: {face_similarity:.1f}%).", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION
    status = "LATE" if is_late else "SUCCESS"
    await log_check_in_attempt(user.id, check_in_type, status, user_location.latitude, user_location.longitude, distance, face_similarity)
    success_message = f"✅ {'Приход' if check_in_type == 'ARRIVAL' else 'Уход'} успешно отмечен!"
    if is_late: success_message += " (с опозданием)"
    await update.message.reply_text(f"{success_message}\n\n📍 Расстояние до офиса: {distance} м.\n👤 Схожесть лица: {face_similarity:.1f}%\n\nХорошего дня!", reply_markup=main_menu_keyboard())
    return CHOOSE_ACTION

async def employee_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Действие отменено.", reply_markup=main_menu_keyboard())
    context.user_data.clear()
    return CHOOSE_ACTION

# --- ДИАЛОГ АДМИНИСТРАТОРА ---
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_user.id not in ADMIN_IDS: return ConversationHandler.END
    await update.message.reply_text("Панель администратора:", reply_markup=admin_menu_keyboard())
    return ADMIN_MENU

async def admin_reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Меню отчетов:", reply_markup=reports_menu_keyboard())
    return ADMIN_REPORTS_MENU

async def admin_get_today_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    today = datetime.now(LOCAL_TIMEZONE).date()
    await send_report_for_period(today, today, context, "Отчет за сегодня", update.effective_chat.id)
    return ADMIN_REPORTS_MENU

async def admin_get_yesterday_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    yesterday = datetime.now(LOCAL_TIMEZONE).date() - timedelta(days=1)
    await send_report_for_period(yesterday, yesterday, context, "Отчет за вчера", update.effective_chat.id)
    return ADMIN_REPORTS_MENU

async def admin_get_weekly_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    today = datetime.now(LOCAL_TIMEZONE).date()
    start_of_week = today - timedelta(days=today.weekday())
    await send_report_for_period(start_of_week, today, context, "Отчет за текущую неделю", update.effective_chat.id)
    return ADMIN_REPORTS_MENU
    
async def admin_custom_report_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Введите период для отчета в формате `ДД.ММ.ГГГГ-ДД.ММ.ГГГГ`\n"
        "Например: `01.06.2024-15.06.2024`",
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return REPORT_GET_DATES

async def admin_custom_report_get_dates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        start_date_str, end_date_str = update.message.text.split('-')
        start_date = datetime.strptime(start_date_str.strip(), '%d.%m.%Y').date()
        end_date = datetime.strptime(end_date_str.strip(), '%d.%m.%Y').date()
        
        if start_date > end_date:
            await update.message.reply_text("Ошибка: Начальная дата не может быть позже конечной. Попробуйте снова.")
            return REPORT_GET_DATES

        await send_report_for_period(start_date, end_date, context, "Отчет за период", update.effective_chat.id)
        await admin_reports_menu(update, context)
        return ADMIN_REPORTS_MENU

    except (ValueError, IndexError):
        await update.message.reply_text("Неверный формат. Пожалуйста, введите даты в формате `ДД.ММ.ГГГГ-ДД.ММ.ГГГГ` и попробуйте снова.")
        return REPORT_GET_DATES

async def admin_export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Подготовка данных для экспорта...")
    all_checkins = await get_all_checkins_for_export()
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Timestamp', 'FullName', 'CheckInType', 'Status', 'Latitude', 'Longitude', 'DistanceMeters', 'FaceSimilarity'])
    writer.writerows(all_checkins)
    csv_bytes = BytesIO(output.getvalue().encode('utf-8'))
    await update.message.reply_document(document=InputFile(csv_bytes, filename=f"checkin_export_{date.today().isoformat()}.csv"), caption="Экспорт всех записей о чек-инах.")
    return ADMIN_REPORTS_MENU

async def admin_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Добавление сотрудника. Шаг 1: Перешлите сообщение от пользователя.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return ADD_GET_ID

async def add_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return ADD_GET_ID
    user_id = update.message.forward_origin.sender_user.id
    context.user_data['new_employee_id'] = user_id
    await update.message.reply_text(f"ID: `{user_id}`\\.\nШаг 2: Введите ФИО\\.", parse_mode='MarkdownV2')
    return ADD_GET_NAME

async def add_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['new_employee_name'] = update.message.text
    await update.message.reply_text(f"Шаг 3: Настройка графика.\nВведите время для {DAYS_OF_WEEK[0]} (формат `09:00-18:00` или `0`).")
    return SCHEDULE_MON

async def admin_modify_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Изменение графика. Перешлите сообщение от сотрудника.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return MODIFY_GET_ID

async def modify_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return MODIFY_GET_ID
    user_id = update.message.forward_origin.sender_user.id
    employee = await get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return MODIFY_GET_ID
    context.user_data['target_employee_id'] = user_id
    context.user_data['target_employee_name'] = employee['name']
    await update.message.reply_text(f"Изменение графика для: {employee['name']}.\nВведите новое время для {DAYS_OF_WEEK[0]} (`09:00-18:00` или `0`).")
    return SCHEDULE_MON

async def admin_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Удаление сотрудника. Перешлите сообщение от сотрудника.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return DELETE_GET_ID

async def delete_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return DELETE_GET_ID
    user_id = update.message.forward_origin.sender_user.id
    employee = await get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return DELETE_GET_ID
    context.user_data['target_employee_id'] = user_id
    await update.message.reply_text(f"Удалить сотрудника {employee['name']} ({user_id})? Это действие деактивирует его доступ.", reply_markup=ReplyKeyboardMarkup([[BUTTON_CONFIRM_DELETE, BUTTON_CANCEL_DELETE]], resize_keyboard=True))
    return DELETE_CONFIRM

async def delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.text == BUTTON_CONFIRM_DELETE:
        await set_employee_active_status(context.user_data['target_employee_id'], False)
        await update.message.reply_text("Сотрудник успешно деактивирован.", reply_markup=admin_menu_keyboard())
    else:
        await update.message.reply_text("Удаление отменено.", reply_markup=admin_menu_keyboard())
    context.user_data.clear()
    return ADMIN_MENU

def schedule_handler_factory(day_index: int):
    async def get_schedule_for_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            parsed_time = parse_day_schedule(update.message.text)
            if parsed_time is None:
                await update.message.reply_text("Неверный формат. Введите время как `чч:мм-чч:мм` или `0`.")
                return SCHEDULE_MON + day_index
            if 'schedule' not in context.user_data: context.user_data['schedule'] = {}
            context.user_data['schedule'][day_index] = parsed_time
            next_day_index = day_index + 1
            if next_day_index < len(DAYS_OF_WEEK):
                await update.message.reply_text(f"Принято. Теперь введите график на {DAYS_OF_WEEK[next_day_index]}.")
                return SCHEDULE_MON + next_day_index
            else:
                full_name, telegram_id = None, None
                if 'new_employee_id' in context.user_data:
                    telegram_id, full_name = context.user_data['new_employee_id'], context.user_data['new_employee_name']
                elif 'target_employee_id' in context.user_data:
                    telegram_id, full_name = context.user_data['target_employee_id'], context.user_data['target_employee_name']
                
                if not (telegram_id and full_name):
                    logger.error("Не удалось определить ID сотрудника при сохранении графика.")
                    await update.message.reply_text("Произошла внутренняя ошибка. Попробуйте снова.", reply_markup=admin_menu_keyboard())
                    context.user_data.clear()
                    return ADMIN_MENU
                
                schedule_data = context.user_data['schedule']
                await add_or_update_employee(telegram_id, full_name, schedule_data)
                
                escaped_name = re.sub(r'([_*\[\]()~`>#\+\-=|{}.!])', r'\\\1', full_name)
                await update.message.reply_text(f"✅ Данные для сотрудника *{escaped_name}* успешно сохранены\\!", parse_mode='MarkdownV2', reply_markup=admin_menu_keyboard())
                context.user_data.clear()
                return ADMIN_MENU
        except Exception as e:
            logger.error(f"Ошибка в schedule_handler_factory: {e}", exc_info=True)
            await update.message.reply_text("Произошла внутренняя ошибка. Процесс отменен.", reply_markup=admin_menu_keyboard())
            context.user_data.clear()
            return ADMIN_MENU
    return get_schedule_for_day

async def admin_back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await admin_command(update, context)
    return ADMIN_MENU

async def main() -> None:
    persistence = PicklePersistence(filepath=PERSISTENCE_FILE)
    application = Application.builder().token(BOT_TOKEN).persistence(persistence).build()
    
    checkin_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start_command)],
        states={
            CHOOSE_ACTION: [MessageHandler(filters.Regex(f"^{BUTTON_ARRIVAL}$"), handle_arrival), MessageHandler(filters.Regex(f"^{BUTTON_DEPARTURE}$"), handle_departure)],
            REGISTER_FACE: [MessageHandler(filters.PHOTO, register_face)],
            AWAITING_PHOTO: [MessageHandler(filters.PHOTO, awaiting_photo)],
            AWAITING_LOCATION: [MessageHandler(filters.LOCATION, awaiting_location)],
        },
        fallbacks=[CommandHandler("cancel", employee_cancel_command)],
        allow_reentry=True, name="checkin_conversation", persistent=True,
    )
    
    schedule_handlers = [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_handler_factory(i)) for i in range(7)]
    admin_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("admin", admin_command)],
        states={
            ADMIN_MENU: [
                MessageHandler(filters.Regex(f"^{BUTTON_ADMIN_ADD}$"), admin_add_start),
                MessageHandler(filters.Regex(f"^{BUTTON_ADMIN_MODIFY}$"), admin_modify_start),
                MessageHandler(filters.Regex(f"^{BUTTON_ADMIN_DELETE}$"), admin_delete_start),
                MessageHandler(filters.Regex(f"^{BUTTON_ADMIN_REPORTS}$"), admin_reports_menu),
            ],
            ADMIN_REPORTS_MENU: [
                MessageHandler(filters.Regex(f"^{BUTTON_REPORT_TODAY}$"), admin_get_today_report),
                MessageHandler(filters.Regex(f"^{BUTTON_REPORT_YESTERDAY}$"), admin_get_yesterday_report),
                MessageHandler(filters.Regex(f"^{BUTTON_REPORT_WEEK}$"), admin_get_weekly_report),
                MessageHandler(filters.Regex(f"^{BUTTON_REPORT_CUSTOM}$"), admin_custom_report_start),
                MessageHandler(filters.Regex(f"^{BUTTON_REPORT_EXPORT}$"), admin_export_csv),
                MessageHandler(filters.Regex(f"^{BUTTON_ADMIN_BACK}$"), admin_command),
            ],
            REPORT_GET_DATES: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_custom_report_get_dates)],
            ADD_GET_ID: [MessageHandler(filters.FORWARDED, add_get_id)],
            ADD_GET_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_get_name)],
            MODIFY_GET_ID: [MessageHandler(filters.FORWARDED, modify_get_id)],
            DELETE_GET_ID: [MessageHandler(filters.FORWARDED, delete_get_id)],
            DELETE_CONFIRM: [MessageHandler(filters.Regex(f"^{BUTTON_CONFIRM_DELETE}$") | filters.Regex(f"^{BUTTON_CANCEL_DELETE}$"), delete_confirm)],
            SCHEDULE_MON: [schedule_handlers[0]], SCHEDULE_TUE: [schedule_handlers[1]], SCHEDULE_WED: [schedule_handlers[2]],
            SCHEDULE_THU: [schedule_handlers[3]], SCHEDULE_FRI: [schedule_handlers[4]], SCHEDULE_SAT: [schedule_handlers[5]],
            SCHEDULE_SUN: [schedule_handlers[6]],
        },
        fallbacks=[MessageHandler(filters.Regex(f"^{BUTTON_ADMIN_BACK}$"), admin_back_to_menu), CommandHandler("cancel", admin_back_to_menu)],
        name="admin_conversation", persistent=True,
    )

    application.add_handler(admin_conv_handler)
    application.add_handler(checkin_conv_handler)
    application.add_handler(CallbackQueryHandler(late_checkin_callback, pattern="^late_checkin$"))
    
    scheduler = AsyncIOScheduler(timezone=LOCAL_TIMEZONE)
    scheduler.add_job(check_and_send_notifications, 'interval', minutes=1, args=[application])
    scheduler.add_job(send_daily_report_job, 'cron', hour=21, minute=0, args=[application])
    
    async with application:
        await init_db()
        await application.initialize()
        await application.updater.start_polling()
        await application.start()
        scheduler.start()
        logger.info("Бот и планировщик запущены. Нажмите Ctrl+C для остановки.")
        await asyncio.Event().wait()

if __name__ == "__main__":
    try: asyncio.run(main())
    except Exception as e: logger.critical(f"Критическая ошибка при запуске: {e}", exc_info=True)
