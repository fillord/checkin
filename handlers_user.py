# handlers_user.py
import logging
import asyncio
import random
import face_recognition
import numpy as np
from datetime import datetime, date, time, timedelta
from io import BytesIO
from concurrent.futures import ProcessPoolExecutor
from app_context import get_process_pool_executor
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from geopy.distance import geodesic
import database
from database import is_day_finished_for_user

from keyboards import main_menu_keyboard
from config import (
    CHOOSE_ACTION, AWAITING_PHOTO, AWAITING_LOCATION, REGISTER_FACE, LIVENESS_ACTIONS,
    BUTTON_ARRIVAL, BUTTON_DEPARTURE, WORK_LOCATION_COORDS, ALLOWED_RADIUS_METERS,
    FACE_DISTANCE_THRESHOLD, AWAITING_LEAVE_REASON, ADMIN_IDS
)

# handlers_user.py
# ... (после всех import)

def _face_recognition_worker(image_bytes: bytes) -> np.ndarray | None:
    """Синхронная функция для поиска и кодирования лица на фото."""
    image = face_recognition.load_image_file(BytesIO(image_bytes))
    face_encodings = face_recognition.face_encodings(image)
    return face_encodings[0] if face_encodings else None

def _face_verification_worker(image_bytes: bytes, known_encoding_bytes: bytes) -> tuple[float, bool]:
    """Синхронная функция для сравнения двух лиц."""
    known_encoding = np.frombuffer(known_encoding_bytes)
    image = face_recognition.load_image_file(BytesIO(image_bytes))

    new_face_encodings = face_recognition.face_encodings(image)
    if not new_face_encodings:
        return 0.0, False

    distance = face_recognition.face_distance([known_encoding], new_face_encodings[0])[0]
    similarity_score = max(0.0, (1.0 - distance) * 100)
    is_match = distance < FACE_DISTANCE_THRESHOLD
    return similarity_score, is_match


logger = logging.getLogger(__name__)

async def verify_face(user_id: int, new_photo_file_id: str, context: ContextTypes.DEFAULT_TYPE) -> tuple[float, bool]:
    employee_data = await database.get_employee_data(user_id)
    if not employee_data or not employee_data["face_encoding"]:
        return 0.0, False

    known_encoding_bytes = employee_data["face_encoding"]
    new_photo_file = await context.bot.get_file(new_photo_file_id)

    photo_stream = BytesIO()
    await new_photo_file.download_to_memory(photo_stream)
    image_bytes = photo_stream.getvalue()

    loop = asyncio.get_running_loop()
    executor = get_process_pool_executor()

    similarity_score, is_match = await loop.run_in_executor(
        executor, _face_verification_worker, image_bytes, known_encoding_bytes
    )

    logger.info(f"Сравнение для {user_id}: схожесть {similarity_score:.2f}%. Результат: {is_match}")
    return similarity_score, is_match

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции start_command из bot.py)
    user = update.effective_user
    employee_data = await database.get_employee_data(user.id)
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
    photo_file = await update.message.photo[-1].get_file()

    photo_stream = BytesIO()
    await photo_file.download_to_memory(photo_stream)
    image_bytes = photo_stream.getvalue()

    await update.message.reply_text("Спасибо. Обрабатываю фото (это может занять несколько секунд)...")

    loop = asyncio.get_running_loop()
    executor = get_process_pool_executor()

    encoding = await loop.run_in_executor(
        executor, _face_recognition_worker, image_bytes
    )

    if encoding is None:
        await update.message.reply_text("Лицо не найдено на фото. Пожалуйста, попробуйте другое, более четкое фото.")
        return REGISTER_FACE

    await database.set_face_encoding(user.id, encoding)
    logger.info(f"Сотрудник {user.id} успешно зарегистрировал эталонное лицо.")
    await update.message.reply_text("Отлично! Ваше лицо зарегистрировано.", reply_markup=main_menu_keyboard())
    return CHOOSE_ACTION


async def handle_arrival(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатие кнопки 'Приход' для своевременных и опоздавших сотрудников."""
    user = update.effective_user

    if await database.has_checked_in_today(user.id, "ARRIVAL"):
        await update.message.reply_text("Вы уже отмечали приход сегодня.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION

    # ПРОВЕРКА: является ли этот чекин опозданием, инициированным ботом?
    is_unhandled_late = user.id in context.bot_data.get('unhandled_late_users', set())

    if is_unhandled_late:
        logger.info(f"Пользователь {user.id} нажал 'Приход' будучи в списке опоздавших. Начинаем late check-in.")
        context.user_data["is_late"] = True
    else:
        # Стандартная проверка по времени для тех, кто нажимает кнопку сам
        schedule = await database.get_employee_today_schedule(user.id)
        if not schedule:
            # Если у сотрудника нет графика на сегодня, ничего не делаем
            return CHOOSE_ACTION

        grace_period_end = (datetime.combine(date.today(), schedule['start_time']) + timedelta(minutes=5)).time()
        if datetime.now(database.LOCAL_TIMEZONE).time() > grace_period_end:
            await update.message.reply_text(f"Вы опоздали. Ваше время для самостоятельного чекина истекло в {grace_period_end.strftime('%H:%M')}.", reply_markup=main_menu_keyboard())
            return CHOOSE_ACTION
        context.user_data["is_late"] = False

    # Общая логика для всех "приходов"
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    await update.message.reply_text(f"Для подтверждения прихода, пожалуйста, {action} и сделайте селфи.", reply_markup=ReplyKeyboardRemove())
    return AWAITING_PHOTO

async def handle_late_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатие кнопки 'Отметиться с опозданием'."""
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    context.user_data["is_late"] = True # Устанавливаем флаг опоздания
    await update.message.reply_text(
        f"Для подтверждения опоздания, пожалуйста, {action} и сделайте селфи.", 
        reply_markup=ReplyKeyboardRemove()
    )
    return AWAITING_PHOTO

async def ask_leave_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс запроса на уход."""
    user_id = update.effective_user.id

    # Используем новую, более надежную проверку
    if await is_day_finished_for_user(user_id):
        await update.message.reply_text("Ваш рабочий день уже завершен.")
        return CHOOSE_ACTION # Остаемся в главном состоянии

    if not await database.has_checked_in_today(user_id, "ARRIVAL"):
        await update.message.reply_text("Вы не можете отпроситься, так как еще не отметили приход сегодня.")
        return CHOOSE_ACTION # Остаемся в главном состоянии

    await update.message.reply_text("Пожалуйста, укажите причину, по которой вы хотите уйти раньше.", reply_markup=ReplyKeyboardRemove())
    return AWAITING_LEAVE_REASON

async def ask_leave_get_reason(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает причину, отправляет запрос админу и возвращает в главное меню."""
    user = update.effective_user
    reason = update.message.text
    employee_data = await database.get_employee_data(user.id)

    logger.info(f"Сотрудник {employee_data['name']} ({user.id}) отпрашивается по причине: {reason}")

    keyboard = [
        [
            InlineKeyboardButton("✅ Разрешить", callback_data=f"leave:approve:{user.id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"leave:deny:{user.id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    text_for_admin = (
        f"❗️ Запрос на уход ❗️\n\n"
        f"Сотрудник: *{employee_data['name']}*\n"
        f"Причина: _{reason}_"
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text_for_admin, reply_markup=reply_markup, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Не удалось отправить запрос админу {admin_id}: {e}")

    await update.message.reply_text("Ваш запрос отправлен администратору. Ожидайте решения.", reply_markup=main_menu_keyboard())

    # --> КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Возвращаем в главное состояние, а не завершаем диалог
    return CHOOSE_ACTION

async def handle_departure(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user

    # Используем новую, более надежную проверку
    if await is_day_finished_for_user(user.id):
        await update.message.reply_text("Вы уже отмечали уход сегодня.")
        return CHOOSE_ACTION

    if not await database.has_checked_in_today(user.id, "ARRIVAL"):
        await update.message.reply_text("Вы не можете отметить уход, так как еще не отметили приход сегодня.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION

    schedule = await database.get_employee_today_schedule(user.id)
    if not schedule:
        return CHOOSE_ACTION

    allowed_departure_start = (datetime.combine(date.today(), schedule['end_time']) - timedelta(minutes=10)).time()
    if datetime.now(database.LOCAL_TIMEZONE).time() < allowed_departure_start:
        await update.message.reply_text(f"Еще слишком рано для ухода. Вы можете отметиться после {allowed_departure_start.strftime('%H:%M')}.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION

    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "DEPARTURE"
    await update.message.reply_text(f"Для подтверждения ухода, пожалуйста, {action} и сделайте селфи.", reply_markup=ReplyKeyboardRemove())
    return AWAITING_PHOTO

async def awaiting_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции awaiting_photo из bot.py)
    context.user_data['photo_file_id'] = update.message.photo[-1].file_id
    location_keyboard = [[KeyboardButton("Отправить мою геолокацию 📍", request_location=True)]]
    await update.message.reply_text("Отлично, фото получил. Теперь, пожалуйста, подтвердите вашу геолокацию.", reply_markup=ReplyKeyboardMarkup(location_keyboard, resize_keyboard=True, one_time_keyboard=True))
    return AWAITING_LOCATION


# handlers_user.py

async def awaiting_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user, user_location = update.effective_user, update.message.location
    photo_file_id = context.user_data.get('photo_file_id')
    check_in_type = context.user_data.get('checkin_type')
    is_late = context.user_data.get('is_late', False)

    # ИЗМЕНЕНИЕ: Вне зависимости от ситуации, в случае ошибки мы всегда возвращаем ГЛАВНУЮ клавиатуру.
    fallback_keyboard = main_menu_keyboard()

    if not all([photo_file_id, check_in_type]):
        await update.message.reply_text("Что-то пошло не так. Начните заново.", reply_markup=main_menu_keyboard())
        context.user_data.clear()
        return CHOOSE_ACTION

    await update.message.reply_text("Геолокация получена. Начинаю проверку...", reply_markup=ReplyKeyboardRemove())
    
    distance = round(geodesic(WORK_LOCATION_COORDS, (user_location.latitude, user_location.longitude)).meters, 2)
    if distance > ALLOWED_RADIUS_METERS:
        await database.log_check_in_attempt(user.id, check_in_type, 'FAIL_LOCATION', user_location.latitude, user_location.longitude, distance)
        await update.message.reply_text(f"❌ Чек-ин отклонен.\nВы находитесь слишком далеко от рабочего места ({distance} м).", reply_markup=fallback_keyboard)
        context.user_data.pop('photo_file_id', None)
        return CHOOSE_ACTION

    face_similarity, is_match = await verify_face(user.id, photo_file_id, context)
    if not is_match:
        await database.log_check_in_attempt(user.id, check_in_type, 'FAIL_FACE', user_location.latitude, user_location.longitude, distance, face_similarity)
        await update.message.reply_text(f"❌ Чек-ин отклонен.\nЛицо на фото не распознано (схожесть: {face_similarity:.1f}%).", reply_markup=fallback_keyboard)
        context.user_data.pop('photo_file_id', None)
        return CHOOSE_ACTION
    
    # В СЛУЧАЕ УСПЕХА
    status = "LATE" if is_late else "SUCCESS"
    await database.log_check_in_attempt(user.id, check_in_type, status, user_location.latitude, user_location.longitude, distance, face_similarity)
    
    if user.id in context.bot_data.get('unhandled_late_users', set()):
        context.bot_data['unhandled_late_users'].remove(user.id)
        logger.info(f"Пользователь {user.id} успешно прошел late-checkin и удален из списка.")

    success_message = f"✅ {'Приход' if check_in_type == 'ARRIVAL' else 'Уход'} успешно отмечен!"
    if is_late: success_message += " (с опозданием)"
    
    await update.message.reply_text(f"{success_message}\n\n📍 Расстояние до офиса: {distance} м.\n👤 Схожесть лица: {face_similarity:.1f}%\n\nХорошего дня!", reply_markup=main_menu_keyboard())
    context.user_data.clear()
    return CHOOSE_ACTION


async def employee_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции employee_cancel_command из bot.py)
    await update.message.reply_text("Действие отменено.", reply_markup=main_menu_keyboard())
    context.user_data.clear()
    return CHOOSE_ACTION


async def late_checkin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции late_checkin_callback из bot.py)
    query = update.callback_query
    await query.answer()
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    context.user_data["is_late"] = True
    await query.edit_message_text(text=f"Вы начали процесс чек-ина с опозданием.\n\nПожалуйста, {action} и сделайте селфи.")
    return AWAITING_PHOTO