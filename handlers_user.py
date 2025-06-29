# handlers_user.py
import logging
import asyncio
import random
import re
import face_recognition
import numpy as np
import database
import config
from datetime import datetime, date, time, timedelta
from io import BytesIO
from concurrent.futures import ProcessPoolExecutor
from app_context import get_process_pool_executor
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from geopy.distance import geodesic
from database import is_day_finished_for_user
from decorators import check_active_employee, user_level_cooldown
from keyboards import main_menu_keyboard, cancel_action_keyboard
from config import (
    CHOOSE_ACTION, AWAITING_PHOTO, AWAITING_LOCATION, REGISTER_FACE, LIVENESS_ACTIONS,
    BUTTON_ARRIVAL, BUTTON_DEPARTURE, WORK_LOCATION_COORDS, ALLOWED_RADIUS_METERS,
    AWAITING_LEAVE_REASON, ADMIN_IDS, AWAITING_NEW_FACE_PHOTO, BUTTON_CANCEL_ACTION
)

def _face_recognition_worker(image_bytes: bytes) -> np.ndarray | None:
    """Синхронная функция для поиска и кодирования лица на фото."""
    image = face_recognition.load_image_file(BytesIO(image_bytes))
    face_encodings = face_recognition.face_encodings(image)
    return face_encodings[0] if face_encodings else None

def _face_verification_worker(image_bytes: bytes, known_encoding_bytes: bytes, threshold: float) -> tuple[float, bool]:
    """Синхронная функция для сравнения двух лиц с заданным порогом."""
    known_encoding = np.frombuffer(known_encoding_bytes)
    image = face_recognition.load_image_file(BytesIO(image_bytes))
    new_face_encodings = face_recognition.face_encodings(image)
    if not new_face_encodings:
        return 0.0, False
    distance = face_recognition.face_distance([known_encoding], new_face_encodings[0])[0]
    similarity_score = max(0.0, (1.0 - distance) * 100)
    is_match = distance < threshold
    return similarity_score, is_match

logger = logging.getLogger(__name__)

async def verify_face(user_id: int, new_photo_file_id: str, context: ContextTypes.DEFAULT_TYPE, custom_threshold: float = None) -> tuple[float, bool]:
    employee_data = await database.get_employee_data(user_id)
    if not employee_data or not employee_data["face_encoding"]:
        return 0.0, False
    known_encoding_bytes = employee_data["face_encoding"]
    new_photo_file = await context.bot.get_file(new_photo_file_id)
    photo_stream = BytesIO()
    await new_photo_file.download_to_memory(photo_stream)
    image_bytes = photo_stream.getvalue()
    threshold_to_use = custom_threshold if custom_threshold is not None else config.FACE_DISTANCE_THRESHOLD_CHECKIN
    loop = asyncio.get_running_loop()
    executor = get_process_pool_executor()
    similarity_score, is_match = await loop.run_in_executor(
        executor, _face_verification_worker, image_bytes, known_encoding_bytes, threshold_to_use
    )
    logger.info(f"Сравнение для {user_id}: схожесть {similarity_score:.2f}%. Порог: < {threshold_to_use}. Результат: {is_match}")
    return similarity_score, is_match

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    employee_data = await database.get_employee_data(user.id)
    if not employee_data:
        await update.message.reply_text("Вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    if not employee_data["face_encoding"]:
        await update.message.reply_text(
            f"Здравствуйте, {employee_data['full_name']}!\n\nНужно зарегистрировать ваше лицо.",
            reply_markup=ReplyKeyboardRemove()
        )
        return config.REGISTER_FACE
    await update.message.reply_text(
        f"Здравствуйте, {employee_data['full_name']}! Выберите действие:",
        reply_markup=main_menu_keyboard()
    )
    return config.CHOOSE_ACTION

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

@check_active_employee
async def handle_arrival(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатие кнопки 'Приход' для своевременных и опоздавших сотрудников."""
    user = update.effective_user

    if await database.has_checked_in_today(user.id, "ARRIVAL"):
        await update.message.reply_text("Вы уже отмечали приход сегодня.", reply_markup=main_menu_keyboard())
        return CHOOSE_ACTION

    schedule = await database.get_employee_today_schedule(user.id)
    if not schedule or not schedule.get('start_time'):
         await update.message.reply_text("Ваш график на сегодня не настроен. Невозможно отметиться.", reply_markup=main_menu_keyboard())
         return CHOOSE_ACTION
    now_local = datetime.now(database.LOCAL_TIMEZONE)
    shift_start_datetime = datetime.combine(now_local.date(), schedule['start_time'], tzinfo=database.LOCAL_TIMEZONE)

    if now_local > shift_start_datetime + timedelta(hours=3):
        await update.message.reply_text(
            "Вы не можете отметиться, так как прошло более 3 часов с начала вашего рабочего дня. "
            "Вы были отмечены как 'прогул'. Пожалуйста, свяжитесь с администратором.",
            reply_markup=main_menu_keyboard()
        )
        await database.mark_as_absent(user.id, now_local.date())
        return CHOOSE_ACTION

    is_unhandled_late = user.id in context.bot_data.get('unhandled_late_users', set())
    if is_unhandled_late:
        logger.info(f"Пользователь {user.id} нажал 'Приход' будучи в списке опоздавших. Начинаем late check-in.")
        context.user_data["is_late"] = True
    else:
        grace_period_end = (datetime.combine(date.today(), schedule['start_time']) + timedelta(minutes=5)).time()
        if datetime.now(database.LOCAL_TIMEZONE).time() > grace_period_end:
            await update.message.reply_text(f"Вы опоздали. Ваше время для самостоятельного чекина истекло в {grace_period_end.strftime('%H:%M')}.", reply_markup=main_menu_keyboard())
            return CHOOSE_ACTION
        context.user_data["is_late"] = False
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    await update.message.reply_text(
        f"Для подтверждения прихода, пожалуйста, {action} и сделайте селфи.",
        reply_markup=cancel_action_keyboard()
    )
    return AWAITING_PHOTO

async def handle_late_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    context.user_data["is_late"] = True
    await update.message.reply_text(
        f"Для подтверждения опоздания, пожалуйста, {action} и сделайте селфи.", 
        reply_markup=cancel_action_keyboard()
    )
    return AWAITING_PHOTO

@check_active_employee
async def ask_leave_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if await is_day_finished_for_user(user_id):
        await update.message.reply_text("Ваш рабочий день уже завершен.")
        return CHOOSE_ACTION 

    if not await database.has_checked_in_today(user_id, "ARRIVAL"):
        await update.message.reply_text("Вы не можете отпроситься, так как еще не отметили приход сегодня.")
        return CHOOSE_ACTION 
    await update.message.reply_text(
        "Пожалуйста, укажите причину, по которой вы хотите уйти раньше.", 
        reply_markup=cancel_action_keyboard()
    )
    return AWAITING_LEAVE_REASON

@check_active_employee
async def ask_leave_get_reason(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    reason = update.message.text
    employee_data = await database.get_employee_data(user.id)
    employee_name = employee_data['full_name']
    logger.info(f"Сотрудник {employee_name} ({user.id}) отпрашивается по причине: {reason}")
    keyboard = [
        [
            InlineKeyboardButton("✅ Разрешить", callback_data=f"leave:approve:{user.id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"leave:deny:{user.id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text_for_admin = (
        f"❗️ Запрос на уход ❗️\n\n"
        f"Сотрудник: *{employee_name}*\n"
        f"Причина: _{reason}_"
    )
    for admin_id in config.ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text_for_admin, reply_markup=reply_markup, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Не удалось отправить запрос админу {admin_id}: {e}")
    await update.message.reply_text("Ваш запрос отправлен администратору. Ожидайте решения.", reply_markup=main_menu_keyboard())
    return config.CHOOSE_ACTION

@user_level_cooldown(60)
@check_active_employee
async def show_my_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    await update.message.reply_text("Загружаю ваш график...")
    employee_data = await database.get_employee_with_schedule(user.id)
    if not employee_data or not employee_data.get('schedule') or not any(v and v != '0' for v in employee_data['schedule'].values()):
        await update.message.reply_text(
            "Ваш график еще не был настроен. Пожалуйста, обратитесь к администратору.",
            reply_markup=main_menu_keyboard()
        )
        return CHOOSE_ACTION
    schedule = employee_data['schedule']
    effective_date = employee_data.get('schedule_effective_date')
    def escape_markdown_v2(text: str) -> str:
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)
    date_str_escaped = ""
    if effective_date:
        date_str = f" (действует с {effective_date.strftime('%d.%m.%Y')})"
        date_str_escaped = escape_markdown_v2(date_str)
    message_lines = [f"*Ваш текущий график*{date_str_escaped}\n"]
    for i, day_name in enumerate(config.DAYS_OF_WEEK):
        day_schedule = schedule.get(i, schedule.get(str(i)))
        if day_schedule and day_schedule != "0":
            schedule_text = escape_markdown_v2(day_schedule)
        else:
            schedule_text = "_Выходной_"
        message_lines.append(f"*{escape_markdown_v2(day_name)}:* {schedule_text}")
    await update.message.reply_text(
        text="\n".join(message_lines),
        parse_mode='MarkdownV2',
        reply_markup=main_menu_keyboard()
    )
    return CHOOSE_ACTION

@check_active_employee
async def handle_departure(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
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
    await update.message.reply_text(
        f"Для подтверждения ухода, пожалуйста, {action} и сделайте селфи.", 
        reply_markup=cancel_action_keyboard()
    )
    return AWAITING_PHOTO

@check_active_employee
async def update_photo_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Вы начали процесс обновления фото.\n\n"
        "Пожалуйста, сделайте и отправьте новое селфи хорошего качества, где хорошо видно ваше лицо.",
        reply_markup=cancel_action_keyboard()
    )
    return AWAITING_NEW_FACE_PHOTO

@check_active_employee
async def update_photo_receive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    new_photo_file_id = update.message.photo[-1].file_id
    await update.message.reply_text("Фото получено. Сравниваю с вашим текущим фото в базе...")
    similarity_score, is_match = await verify_face(
        user.id,
        new_photo_file_id,
        context,
        custom_threshold=config.FACE_DISTANCE_THRESHOLD_UPDATE
    )
    if not is_match:
        await update.message.reply_text(
            f"❌ Обновление отклонено.\nЛицо на новом фото не совпадает с вашим профилем (схожесть: {similarity_score:.1f}%).\n"
            "Попробуйте еще раз или обратитесь к администратору.",
            reply_markup=main_menu_keyboard()
        )
        return CHOOSE_ACTION
    await update.message.reply_text("Верификация пройдена. Сохраняю новое фото...")
    try:
        photo_file = await context.bot.get_file(new_photo_file_id)
        photo_stream = BytesIO()
        await photo_file.download_to_memory(photo_stream)
        image_bytes = photo_stream.getvalue()
        loop = asyncio.get_running_loop()
        executor = get_process_pool_executor()
        new_encoding = await loop.run_in_executor(
            executor, _face_recognition_worker, image_bytes
        )
        if new_encoding is None:
            await update.message.reply_text("Не удалось распознать лицо на новом фото. Попробуйте еще раз.", reply_markup=main_menu_keyboard())
            return CHOOSE_ACTION
        await database.set_face_encoding(user.id, new_encoding)
        logger.info(f"Сотрудник {user.id} успешно обновил свое эталонное фото.")
        await update.message.reply_text("✅ Ваше фото в профиле успешно обновлено!", reply_markup=main_menu_keyboard())
    except Exception as e:
        logger.error(f"Критическая ошибка при обновлении фото для {user.id}: {e}")
        await update.message.reply_text("Произошла внутренняя ошибка при сохранении фото. Попробуйте позже.", reply_markup=main_menu_keyboard())
    return CHOOSE_ACTION

@check_active_employee
async def awaiting_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['photo_file_id'] = update.message.photo[-1].file_id
    location_keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton("Отправить мою геолокацию 📍", request_location=True)],
            [KeyboardButton(BUTTON_CANCEL_ACTION)]
        ], 
        resize_keyboard=True
    )
    await update.message.reply_text(
        "Отлично, фото получил. Теперь, пожалуйста, подтвердите вашу геолокацию.", 
        reply_markup=location_keyboard
    )
    return AWAITING_LOCATION

@check_active_employee
async def awaiting_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user, user_location = update.effective_user, update.message.location
    photo_file_id = context.user_data.get('photo_file_id')
    check_in_type = context.user_data.get('checkin_type')
    is_late = context.user_data.get('is_late', False)
    fallback_keyboard = main_menu_keyboard()
    if not all([photo_file_id, check_in_type]):
        await update.message.reply_text("Что-то пошло не так. Начните заново.", reply_markup=main_menu_keyboard())
        context.user_data.clear()
        return CHOOSE_ACTION
    await update.message.reply_text("Геолокация получена. Начинаю проверку...", reply_markup=ReplyKeyboardRemove())
    user_coords = (user_location.latitude, user_location.longitude)
    distances = [geodesic(office_coords, user_coords).meters for office_coords in WORK_LOCATION_COORDS]
    min_distance = round(min(distances), 2)
    if min_distance > ALLOWED_RADIUS_METERS:
        await database.log_check_in_attempt(user.id, check_in_type, 'FAIL_LOCATION', user_location.latitude, user_location.longitude, min_distance)
        await update.message.reply_text(f"❌ Чек-ин отклонен.\nВы находитесь слишком далеко от рабочего места ({min_distance} м).", reply_markup=fallback_keyboard)
        context.user_data.pop('photo_file_id', None)
        return CHOOSE_ACTION
    face_similarity, is_match = await verify_face(user.id, photo_file_id, context)
    if not is_match:
        await database.log_check_in_attempt(user.id, check_in_type, 'FAIL_FACE', user_location.latitude, user_location.longitude, min_distance, face_similarity)
        await update.message.reply_text(f"❌ Чек-ин отклонен.\nЛицо на фото не распознано (схожесть: {face_similarity:.1f}%).", reply_markup=fallback_keyboard)
        context.user_data.pop('photo_file_id', None)
        return CHOOSE_ACTION
    status = "LATE" if is_late else "SUCCESS"
    await database.log_check_in_attempt(user.id, check_in_type, status, user_location.latitude, user_location.longitude, min_distance, face_similarity)
    if user.id in context.bot_data.get('unhandled_late_users', set()):
        context.bot_data['unhandled_late_users'].remove(user.id)
        logger.info(f"Пользователь {user.id} успешно прошел late-checkin и удален из списка.")
    success_message = f"✅ {'Приход' if check_in_type == 'ARRIVAL' else 'Уход'} успешно отмечен!"
    if is_late: success_message += " (с опозданием)"
    await update.message.reply_text(f"{success_message}\n\n📍 Расстояние до ближайшего офиса: {min_distance} м.\n👤 Схожесть лица: {face_similarity:.1f}%\n\nХорошего дня!", reply_markup=main_menu_keyboard())
    context.user_data.clear()
    return CHOOSE_ACTION

@check_active_employee
@user_level_cooldown(60)
async def get_personal_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text("Собираю вашу статистику за текущий месяц...")
    stats = await database.get_personal_monthly_stats(user_id)
    month_name = datetime.now(database.LOCAL_TIMEZONE).strftime('%B')
    report_text = (
        f"📊 *Статистика за {month_name}*:\n\n"
        f"✅ *Рабочих дней отмечено:* {stats['work_days']}\n"
        f"🕒 *Опозданий:* {stats['late_days']}\n"
        f"🙏 *Отпросился раньше:* {stats['left_early_days']}"
    )
    await update.message.reply_text(report_text, parse_mode='Markdown')

async def employee_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Действие отменено.", reply_markup=main_menu_keyboard())
    context.user_data.clear()
    return CHOOSE_ACTION

async def late_checkin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    action = random.choice(LIVENESS_ACTIONS)
    context.user_data["checkin_type"] = "ARRIVAL"
    context.user_data["is_late"] = True
    await query.edit_message_text(text=f"Вы начали процесс чек-ина с опозданием.\n\nПожалуйста, {action} и сделайте селфи.")
    return AWAITING_PHOTO