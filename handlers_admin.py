# handlers_admin.py
import logging
import re
import csv
import database
import config

from datetime import time, datetime, date, timedelta
from io import StringIO, BytesIO

from telegram import Update, ReplyKeyboardMarkup, InputFile, MessageOriginUser, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import ContextTypes, ConversationHandler

from jobs import send_report_for_period
from keyboards import admin_menu_keyboard, reports_menu_keyboard, leave_type_keyboard, holidays_menu_keyboard
from keyboards import (
    BUTTON_LEAVE_TYPE_VACATION, BUTTON_LEAVE_TYPE_SICK
)

from config import (
    ADMIN_IDS, ADMIN_MENU, ADMIN_REPORTS_MENU, REPORT_GET_DATES, MONTHLY_CSV_GET_MONTH,
    ADD_GET_ID, ADD_GET_NAME, MODIFY_GET_ID, DELETE_GET_ID, DELETE_CONFIRM,
    SCHEDULE_MON, DAYS_OF_WEEK, BUTTON_ADMIN_BACK, BUTTON_CONFIRM_DELETE, BUTTON_CANCEL_DELETE,
    LEAVE_GET_ID, LEAVE_GET_TYPE, LEAVE_GET_PERIOD, CANCEL_LEAVE_GET_ID, CANCEL_LEAVE_GET_PERIOD,
    SCHEDULE_GET_EFFECTIVE_DATE, LOCAL_TIMEZONE,
    HOLIDAY_MENU, HOLIDAY_GET_ADD_DATE, HOLIDAY_GET_ADD_NAME, HOLIDAY_GET_DELETE_DATE, AWAITING_SCHEDULE_FILE, AWAITING_ADD_EMPLOYEES_FILE
)

logger = logging.getLogger(__name__)

def parse_day_schedule(text: str) -> dict | None:
    # ... (скопируйте сюда содержимое функции parse_day_schedule из bot.py)
    text = text.strip().lower()
    if text in ("0", "выходной"): return {}
    time_pattern = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$")
    match = time_pattern.match(text)
    if match: return {"start": f"{match.group(1)}:{match.group(2)}", "end": f"{match.group(3)}:{match.group(4)}"}
    return None

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_command из bot.py)
    if update.effective_user.id not in ADMIN_IDS: return ConversationHandler.END
    await update.message.reply_text("Панель администратора:", reply_markup=admin_menu_keyboard())
    return ADMIN_MENU

async def admin_reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_reports_menu из bot.py)
    await update.message.reply_text("Меню отчетов:", reply_markup=reports_menu_keyboard())
    return ADMIN_REPORTS_MENU

async def admin_get_today_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_get_today_report из bot.py)
    today = datetime.now(database.LOCAL_TIMEZONE).date()
    await send_report_for_period(today, today, context, "Отчет за сегодня", update.effective_chat.id)
    return ADMIN_REPORTS_MENU

async def admin_get_yesterday_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_get_yesterday_report из bot.py)
    yesterday = datetime.now(database.LOCAL_TIMEZONE).date() - timedelta(days=1)
    await send_report_for_period(yesterday, yesterday, context, "Отчет за вчера", update.effective_chat.id)
    return ADMIN_REPORTS_MENU

async def admin_get_weekly_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_get_weekly_report из bot.py)
    today = datetime.now(database.LOCAL_TIMEZONE).date()
    start_of_week = today - timedelta(days=today.weekday())
    await send_report_for_period(start_of_week, today, context, "Отчет за текущую неделю", update.effective_chat.id)
    return ADMIN_REPORTS_MENU

async def admin_custom_report_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_custom_report_start из bot.py)
    await update.message.reply_text(
        "Введите период для отчета в формате `ДД.ММ.ГГГГ-ДД.ММ.ГГГГ`\n"
        "Например: `01.06.2024-15.06.2024`",
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return REPORT_GET_DATES

async def admin_custom_report_get_dates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_custom_report_get_dates из bot.py)
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
    # ... (скопируйте сюда содержимое функции admin_export_csv из bot.py)
    await update.message.reply_text("Подготовка данных для экспорта...")
    all_checkins = await database.get_all_checkins_for_export()
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Timestamp', 'FullName', 'CheckInType', 'Status', 'Latitude', 'Longitude', 'DistanceMeters', 'FaceSimilarity'])
    writer.writerows(all_checkins)
    csv_bytes = BytesIO(output.getvalue().encode('utf-8'))
    await update.message.reply_document(document=InputFile(csv_bytes, filename=f"checkin_export_{date.today().isoformat()}.csv"), caption="Экспорт всех записей о чек-инах.")
    return ADMIN_REPORTS_MENU

async def admin_monthly_csv_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_monthly_csv_start из bot.py)
    await update.message.reply_text(
        "Введите месяц и год для сводного отчета в формате `ММ.ГГГГ`\n"
        "Например: `06.2025`",
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True),
        parse_mode='MarkdownV2'
    )
    return MONTHLY_CSV_GET_MONTH

async def admin_monthly_csv_get_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        month_str, year_str = update.message.text.strip().split('.')
        month, year = int(month_str), int(year_str)

        await update.message.reply_text(f"Пожалуйста, подождите, идет формирование сводного отчета за {month:02d}.{year}...")
        
        summary_data = await database.get_monthly_summary_data(year, month)

        if not summary_data or len(summary_data) <= 1:
            await update.message.reply_text("Нет данных для формирования отчета за указанный период.")
            await admin_reports_menu(update, context)
            return config.ADMIN_REPORTS_MENU

        output = StringIO()
        writer = csv.writer(output)
        writer.writerows(summary_data)
        
        csv_bytes = BytesIO(output.getvalue().encode('utf-8'))
        filename = f"monthly_summary_{year}_{month:02d}.csv"
        
        await update.message.reply_document(
            document=InputFile(csv_bytes, filename=filename),
            caption=f"Сводный отчет по посещаемости за {month:02d}.{year}"
        )
        await admin_reports_menu(update, context)
        return config.ADMIN_REPORTS_MENU

    except (ValueError, IndexError):
        # --> ИСПРАВЛЕНИЕ ЗДЕСЬ: Экранируем точки в сообщении об ошибке
        error_text = "Неверный формат\\. Пожалуйста, введите месяц и год как `ММ\\.ГГГГ` и попробуйте снова\\."
        await update.message.reply_text(
            text=error_text,
            parse_mode='MarkdownV2'
        )
        return config.MONTHLY_CSV_GET_MONTH

async def handle_leave_request_decision(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает решение администратора по запросу на уход."""
    query = update.callback_query
    await query.answer()

    try:
        _, action, user_id_str = query.data.split(':')
        user_id = int(user_id_str)
    except (ValueError, IndexError):
        await query.edit_message_text("Ошибка! Не удалось обработать запрос.")
        return

    employee_data = await database.get_employee_data(user_id)
    if not employee_data:
        await query.edit_message_text(f"Ошибка! Сотрудник с ID {user_id} не найден.")
        return

    original_text = query.message.text

    if action == 'approve':
        # Автоматически отмечаем уход
        await database.log_check_in_attempt(user_id, 'SYSTEM_LEAVE', 'APPROVED_LEAVE')

        await query.edit_message_text(text=f"{original_text}\n\n✅ *ВЫ РАЗРЕШИЛИ УХОД*", parse_mode='Markdown')
        try:
            await context.bot.send_message(chat_id=user_id, text="✅ Ваш запрос на уход одобрен. Ваш рабочий день завершен.")
        except Exception as e:
            logger.error(f"Не удалось уведомить сотрудника {user_id} об одобрении: {e}")

    elif action == 'deny':
        await query.edit_message_text(text=f"{original_text}\n\n❌ *ВЫ ОТКЛОНИЛИ ЗАПРОС*", parse_mode='Markdown')
        try:
            await context.bot.send_message(chat_id=user_id, text="❌ В вашем запросе на уход было отказано. Не забудьте отметиться в конце рабочего дня.")
        except Exception as e:
            logger.error(f"Не удалось уведомить сотрудника {user_id} об отказе: {e}")

async def admin_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_add_start из bot.py)
    context.user_data.clear()
    await update.message.reply_text("Добавление сотрудника. Шаг 1: Перешлите сообщение от пользователя.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return ADD_GET_ID

async def add_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции add_get_id из bot.py)
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return ADD_GET_ID
    user_id = update.message.forward_origin.sender_user.id
    context.user_data['new_employee_id'] = user_id
    await update.message.reply_text(f"ID: `{user_id}`\\.\nШаг 2: Введите ФИО\\.", parse_mode='MarkdownV2')
    return ADD_GET_NAME

async def add_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['new_employee_name'] = update.message.text
    
    # --> ИЗМЕНЕНИЕ: Добавлено экранирование всех точек
    text_to_send = (
        "ФИО принято\\.\n"
        "С какой даты будет действовать график? Введите дату в формате `ДД\\.ММ\\.ГГГГ` или напишите `сегодня`\\."
    )
    
    await update.message.reply_text(
        text=text_to_send,
        parse_mode='MarkdownV2'
    )
    return config.SCHEDULE_GET_EFFECTIVE_DATE

async def admin_modify_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_modify_start из bot.py)
    context.user_data.clear()
    await update.message.reply_text("Изменение графика. Перешлите сообщение от сотрудника.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return MODIFY_GET_ID

async def modify_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return config.MODIFY_GET_ID
        
    user_id = update.message.forward_origin.sender_user.id
    employee = await database.get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return config.MODIFY_GET_ID
        
    context.user_data['target_employee_id'] = user_id
    context.user_data['target_employee_name'] = employee['full_name']
    
    text_to_send = (
        f"Изменение графика для: {employee['full_name']}\\.\n"
        f"С какой даты будет действовать новый график? Введите дату в формате `ДД\\.ММ\\.ГГГГ` или напишите `сегодня`\\."
    )
    
    await update.message.reply_text(
        text=text_to_send,
        parse_mode='MarkdownV2',
        reply_markup=ReplyKeyboardMarkup([[config.BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return config.SCHEDULE_GET_EFFECTIVE_DATE

async def schedule_get_effective_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает дату вступления в силу графика и проверяет, что она не в прошлом."""
    text = update.message.text.strip().lower()
    effective_date = None
    today = datetime.now(config.LOCAL_TIMEZONE).date()

    if text == 'сегодня':
        effective_date = today
    else:
        try:
            effective_date = datetime.strptime(text, '%d.%m.%Y').date()
        except ValueError:
            await update.message.reply_text("Неверный формат. Введите дату как `ДД.ММ.ГГГГ` или `сегодня`.", parse_mode='MarkdownV2')
            return config.SCHEDULE_GET_EFFECTIVE_DATE

    # --> НОВАЯ ПРОВЕРКА: Убедимся, что дата не в прошлом
    if effective_date < today:
        await update.message.reply_text("❌ Ошибка: Нельзя устанавливать или изменять график для прошедших дат. Пожалуйста, введите сегодняшнюю или будущую дату.")
        return config.SCHEDULE_GET_EFFECTIVE_DATE # Возвращаемся на этот же шаг для повторного ввода

    context.user_data['schedule_effective_date'] = effective_date
    await update.message.reply_text(f"График будет действовать с {effective_date.strftime('%d.%m.%Y')}.\nТеперь введите время для {config.DAYS_OF_WEEK[0]} (`09:00-18:00` или `0`).")
    return config.SCHEDULE_MON

async def admin_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_delete_start из bot.py)
    context.user_data.clear()
    await update.message.reply_text("Удаление сотрудника. Перешлите сообщение от сотрудника.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return DELETE_GET_ID

async def delete_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции delete_get_id из bot.py)
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return DELETE_GET_ID
    user_id = update.message.forward_origin.sender_user.id
    employee = await database.get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return DELETE_GET_ID
    context.user_data['target_employee_id'] = user_id
    await update.message.reply_text(f"Удалить сотрудника {employee['full_name']} ({user_id})? Это действие деактивирует его доступ.", reply_markup=ReplyKeyboardMarkup([[BUTTON_CONFIRM_DELETE, BUTTON_CANCEL_DELETE]], resize_keyboard=True))
    return DELETE_CONFIRM

async def delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции delete_confirm из bot.py)
    if update.message.text == BUTTON_CONFIRM_DELETE:
        await database.set_employee_active_status(context.user_data['target_employee_id'], False)
        await update.message.reply_text("Сотрудник успешно деактивирован.", reply_markup=admin_menu_keyboard())
    else:
        await update.message.reply_text("Удаление отменено.", reply_markup=admin_menu_keyboard())
    context.user_data.clear()
    return ADMIN_MENU


# --- НОВЫЙ БЛОК ДЛЯ МАССОВОГО ДОБАВЛЕНИЯ СОТРУДНИКОВ ---

async def bulk_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс массового добавления сотрудников."""
    instructions = (
        "Вы начали процесс *массового добавления и обновления сотрудников*.\n\n"
        "Пожалуйста, подготовьте и отправьте CSV-файл. Если сотрудник с указанным `telegram_id` уже существует, его график будет обновлен. Если нет — будет создан новый сотрудник.\n\n"
        "**Формат файла точно такой же, как для обновления графиков:**\n"
        "`telegram_id,effective_from_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday`\n\n"
        "Отправьте файл, чтобы начать."
    )
    await update.message.reply_text(instructions, parse_mode='Markdown')
    return AWAITING_ADD_EMPLOYEES_FILE


async def handle_add_employees_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает файл для добавления/обновления сотрудников."""
    document = update.message.document
    if not document or not document.file_name.endswith('.csv'):
        await update.message.reply_text("Пожалуйста, отправьте файл в формате .csv")
        return AWAITING_ADD_EMPLOYEES_FILE

    await update.message.reply_text("Файл получен. Начинаю обработку...")

    file = await document.get_file()
    file_content_bytes = await file.download_as_bytearray()
    
    try:
        file_content_str = file_content_bytes.decode('utf-8')
    except UnicodeDecodeError:
        await update.message.reply_text("Ошибка: файл должен быть в кодировке UTF-8.")
        return ConversationHandler.END

    csvfile = StringIO(file_content_str)
    # Используем DictReader для удобной работы со столбцами по их именам
    # strip() убирает случайные пробелы в заголовках
    reader = csv.DictReader(l.strip() for l in csvfile)
    
    schedules_to_update = []
    errors = []
    time_pattern = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$")

    for i, row in enumerate(reader, start=2):
        try:
            # Проверяем наличие обязательных полей
            required_fields = ['telegram_id', 'effective_from_date', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            if not all(field in row for field in required_fields):
                raise ValueError("Отсутствуют необходимые столбцы. Проверьте заголовок файла.")

            telegram_id = int(row['telegram_id'].strip())
            effective_date = datetime.strptime(row['effective_from_date'].strip(), '%d.%m.%Y').date()
            
            schedule = {}
            days_of_week_keys = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            for day_index, day_key in enumerate(days_of_week_keys):
                schedule_str = row[day_key].strip()
                if schedule_str == '0':
                    schedule[day_index] = {}
                elif time_pattern.match(schedule_str):
                    start_str, end_str = schedule_str.split('-')
                    schedule[day_index] = {'start': time.fromisoformat(start_str), 'end': time.fromisoformat(end_str)}
                else:
                    raise ValueError(f"неверный формат времени в столбце '{day_key}'")
            
            # Для добавления сотрудника нужно его имя, которого нет в файле.
            # Мы будем использовать заглушку "Требуется обновить ФИО"
            # и вызывать существующую функцию add_or_update_employee
            # В файле с графиками нет ФИО, поэтому нужно получить его или установить заглушку
            # Давайте упростим: эта функция будет только для ОБНОВЛЕНИЯ графиков. 
            # Для добавления нужно ФИО, которого нет в файле.
            # Переосмыслим: Чтобы добавлять, нужно имя.
            # Давайте изменим формат файла!

            # --- ПЕРЕСМОТР ЛОГИКИ ---
            # Для добавления нужно имя. Добавим его в CSV.
            # Новый формат: telegram_id,full_name,effective_from_date,...
            
            # --- Возвращаемся к исходному плану, но с правкой ---
            # Логика handle_schedule_file была почти идеальна, используем ее
            # как шаблон, но с фокусом на добавление.
            # Для добавления нужно имя, которого нет в файле с графиками.
            # Давайте сделаем так: если юзер существует, обновляем график.
            # Если нет - сообщаем об ошибке, что юзера нужно сперва добавить.
            # Это самый безопасный путь.

            # Поэтому оставляем логику handle_schedule_file как есть,
            # и создаем новый обработчик для добавления с ДРУГИМ форматом файла.
            
            # --- НОВЫЙ, ПРАВИЛЬНЫЙ ПОДХОД ---
            
            # Логика `handle_add_employees_file` должна отличаться.
            # Файл для добавления должен содержать ФИО.
            # Новый формат: telegram_id, full_name, effective_from_date, ...
            
            if 'full_name' not in row or not row['full_name'].strip():
                 raise ValueError("Отсутствует или пустое ФИО (full_name)")
            
            full_name = row['full_name'].strip()
            
            # Собираем данные для add_or_update_employee
            schedules_to_update.append({
                'telegram_id': telegram_id,
                'full_name': full_name, # <-- Добавили ФИО
                'effective_date': effective_date,
                'schedule': schedule
            })

        except (ValueError, IndexError, KeyError) as e:
            errors.append(f"Строка {i}: Ошибка - {e}. Данные: `{','.join(row.values())}`")

    # Массовое обновление/добавление в БД
    if schedules_to_update:
        # Здесь мы не можем использовать bulk_add_or_update_schedules, так как она не принимает full_name
        # Нам нужно либо модифицировать ее, либо вызывать add_or_update_employee в цикле.
        # Давайте модифицируем `database.py` для поддержки этого.
        # НЕТ, `add_or_update_employee` - идеальный вариант для этого.
        # Будем вызывать его в цикле. Для 50-100 записей это приемлемо.
        for employee_data in schedules_to_update:
            await database.add_or_update_employee(
                telegram_id=employee_data['telegram_id'],
                full_name=employee_data['full_name'],
                schedule_data=employee_data['schedule'],
                effective_date=employee_data['effective_date']
            )
            
    # Отправка отчета
    success_count = len(schedules_to_update)
    error_count = len(errors)
    
    summary = f"Обработка файла завершена.\n\n✅ Успешно добавлено/обновлено: *{success_count}* сотрудников.\n❌ Обнаружено ошибок: *{error_count}*."
    await update.message.reply_text(summary, parse_mode='Markdown')

    if errors:
        error_report_str = "Детализация ошибок:\n\n" + "\n".join(errors)
        error_report_bytes = error_report_str.encode('utf-8')
        error_file = BytesIO(error_report_bytes)
        await update.message.reply_document(
            document=InputFile(error_file, filename='add_employees_error_report.txt'),
            caption="Найдены ошибки. Исправьте их в исходном файле и отправьте его снова."
        )

    return ConversationHandler.END


# --- КОНЕЦ НОВОГО БЛОКА ---

# --- НОВЫЙ БЛОК ДЛЯ МАССОВОГО ОБНОВЛЕНИЯ ГРАФИКОВ ---

async def bulk_update_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс массового обновления графиков."""
    # --- ИСПРАВЛЕННЫЙ ТЕКСТ ИНСТРУКЦИИ ---
    instructions = (
        "Вы начали процесс массового обновления графиков.\n\n"
        "Пожалуйста, подготовьте и отправьте мне CSV-файл со следующими столбцами:\n"
        "`telegram_id,effective_from_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday`\n\n"
        "*Требования:*\n"
        "1. *Кодировка файла:* UTF-8.\n"
        "2. *Разделитель:* запятая (,)\n"
        "3. *telegram_id:* Числовой ID пользователя в Telegram.\n"
        "4. *effective_from_date:* Дата вступления графика в силу в формате `ДД.ММ.ГГГГ`.\n"
        "5. *Дни недели:* Время в формате `ЧЧ:ММ-ЧЧ:ММ` или `0` для выходного.\n\n"
        "Отправьте файл в этот чат, чтобы начать обработку."
    )
    await update.message.reply_text(instructions, parse_mode='Markdown')
    return AWAITING_SCHEDULE_FILE


async def handle_schedule_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает полученный CSV-файл с графиками."""
    document = update.message.document
    if not document or not document.file_name.endswith('.csv'):
        await update.message.reply_text("Пожалуйста, отправьте файл в формате .csv")
        return AWAITING_SCHEDULE_FILE

    await update.message.reply_text("Файл получен. Начинаю обработку, это может занять некоторое время...")

    file = await document.get_file()
    file_content_bytes = await file.download_as_bytearray()
    
    try:
        file_content_str = file_content_bytes.decode('utf-8')
    except UnicodeDecodeError:
        await update.message.reply_text("Ошибка: файл должен быть в кодировке UTF-8. Пожалуйста, пересохраните файл и попробуйте снова.")
        return ConversationHandler.END

    csvfile = StringIO(file_content_str)
    reader = csv.reader(csvfile)

    schedules_to_update = []
    errors = []
    time_pattern = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$")

    try:
        header = next(reader) # Пропускаем заголовок
        for i, row in enumerate(reader, start=2): # Начинаем нумерацию со строки 2
            if not row: continue # Пропускаем пустые строки
            
            try:
                if len(row) != 9:
                    raise ValueError("Неверное количество столбцов (должно быть 9)")

                telegram_id = int(row[0].strip())
                effective_date = datetime.strptime(row[1].strip(), '%d.%m.%Y').date()
                
                schedule = {}
                for day_index in range(7):
                    schedule_str = row[day_index + 2].strip()
                    if schedule_str == '0':
                        schedule[day_index] = {}
                    elif time_pattern.match(schedule_str):
                        start_str, end_str = schedule_str.split('-')
                        schedule[day_index] = {'start': time.fromisoformat(start_str), 'end': time.fromisoformat(end_str)}
                    else:
                        raise ValueError(f"неверный формат времени в столбце '{header[day_index+2]}'")
                
                schedules_to_update.append({
                    'telegram_id': telegram_id,
                    'effective_date': effective_date,
                    'schedule': schedule
                })

            except (ValueError, IndexError) as e:
                errors.append(f"Строка {i}: Ошибка - {e}. Данные: `{','.join(row)}`")

    except Exception as e:
        await update.message.reply_text(f"Критическая ошибка при чтении файла: {e}")
        return ConversationHandler.END

    if schedules_to_update:
        try:
            await database.bulk_add_or_update_schedules(schedules_to_update)
        except Exception as e:
            await update.message.reply_text(f"Произошла ошибка при обновлении данных в базе: {e}")
            return ConversationHandler.END
            
    success_count = len(schedules_to_update)
    error_count = len(errors)
    
    summary = f"Обработка файла завершена.\n\n✅ Успешно обработано и готово к обновлению: *{success_count}* записей.\n❌ Обнаружено ошибок: *{error_count}*."
    await update.message.reply_text(summary, parse_mode='Markdown')

    if errors:
        error_report_str = "Детализация ошибок:\n\n" + "\n".join(errors)
        error_report_bytes = error_report_str.encode('utf-8')
        error_file = BytesIO(error_report_bytes)
        await update.message.reply_document(
            document=InputFile(error_file, filename='error_report.txt'),
            caption="Найдены ошибки. Исправьте их в исходном файле и отправьте его снова."
        )

    return ConversationHandler.END

# --- КОНЕЦ НОВОГО БЛОКА ---

# --- НОВЫЙ БЛОК: УПРАВЛЕНИЕ ПРАЗДНИКАМИ ---

async def admin_holidays_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отображает меню управления праздниками."""
    await update.message.reply_text(
        "Выберите действие:",
        reply_markup=holidays_menu_keyboard()
    )
    return HOLIDAY_MENU

async def holiday_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс добавления праздника."""
    await update.message.reply_text(
        "Введите дату нового праздника в формате `ДД.ММ.ГГГГ`:",
        parse_mode='MarkdownV2',
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return HOLIDAY_GET_ADD_DATE

async def holiday_get_add_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает дату и запрашивает название праздника."""
    try:
        holiday_date = datetime.strptime(update.message.text.strip(), '%d.%m.%Y').date()
        context.user_data['holiday_date'] = holiday_date
        await update.message.reply_text("Отлично. Теперь введите название праздника (например, 'Новый год'):")
        return HOLIDAY_GET_ADD_NAME
    except ValueError:
        await update.message.reply_text("Неверный формат. Пожалуйста, введите дату как `ДД.ММ.ГГГГ` и попробуйте снова.", parse_mode='MarkdownV2')
        return HOLIDAY_GET_ADD_DATE

async def holiday_get_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает название, сохраняет праздник и завершает диалог."""
    holiday_name = update.message.text.strip()
    holiday_date = context.user_data.get('holiday_date')

    if not holiday_name or not holiday_date:
        await update.message.reply_text("Произошла ошибка, попробуйте снова.", reply_markup=admin_menu_keyboard())
        context.user_data.clear()
        return ADMIN_MENU

    await database.add_holiday(holiday_date, holiday_name)
    
    await update.message.reply_text(
        f"✅ Праздник '{holiday_name}' на дату {holiday_date.strftime('%d.%m.%Y')} успешно добавлен!",
        reply_markup=admin_menu_keyboard()
    )
    context.user_data.clear()
    return ADMIN_MENU

async def holiday_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс удаления праздника."""
    await update.message.reply_text(
        "Введите дату праздника для удаления в формате `ДД.ММ.ГГГГ`:",
        parse_mode='MarkdownV2',
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return HOLIDAY_GET_DELETE_DATE

async def holiday_get_delete_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает дату, удаляет праздник и завершает диалог."""
    try:
        holiday_date = datetime.strptime(update.message.text.strip(), '%d.%m.%Y').date()
        await database.delete_holiday(holiday_date)
        await update.message.reply_text(
            f"✅ Праздник на дату {holiday_date.strftime('%d.%m.%Y')} (если он существовал) был удален.",
            reply_markup=admin_menu_keyboard()
        )
        context.user_data.clear()
        return ADMIN_MENU
    except ValueError:
        await update.message.reply_text("Неверный формат. Пожалуйста, введите дату как `ДД.ММ.ГГГГ` и попробуйте снова.", parse_mode='MarkdownV2')
        return HOLIDAY_GET_DELETE_DATE
# --- КОНЕЦ НОВОГО БЛОКА ---

def schedule_handler_factory(day_index: int):
    # ... (скопируйте сюда содержимое функции schedule_handler_factory из bot.py)
    async def get_schedule_for_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            parsed_time = parse_day_schedule(update.message.text)
            if parsed_time is None:
                await update.message.reply_text("Неверный формат. Введите время как `чч:мм-чч:мм` или `0`.")
                return SCHEDULE_MON + day_index
            if 'schedule' not in context.user_data: context.user_data['schedule'] = {}
            if parsed_time:
                parsed_time['start'] = time.fromisoformat(parsed_time['start'])
                parsed_time['end'] = time.fromisoformat(parsed_time['end'])

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
                logger.info(f"ПОПЫТКА СОХРАНЕНИЯ ГРАФИКА. Данные: {context.user_data.get('schedule')}")
                
                schedule_data = context.user_data['schedule']
                effective_date = context.user_data['schedule_effective_date']
                await database.add_or_update_employee(telegram_id, full_name, schedule_data, effective_date)
                
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

async def admin_add_leave_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 1: Просит переслать сообщение от сотрудника."""
    await update.message.reply_text(
        "Назначение отсутствия. Перешлите сообщение от сотрудника, которому нужно назначить отпуск/больничный.",
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return LEAVE_GET_ID

async def admin_add_leave_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 2: Получает ID, просит выбрать тип отсутствия."""
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return config.LEAVE_GET_ID
        
    user_id = update.message.forward_origin.sender_user.id
    employee = await database.get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return config.LEAVE_GET_ID
    
    # ИСПРАВЛЕНИЕ: Используем 'full_name'
    context.user_data['leave_employee_id'] = user_id
    context.user_data['leave_employee_name'] = employee['full_name']
    
    await update.message.reply_text(
        # ИСПРАВЛЕНИЕ: Используем 'full_name'
        f"Выбран сотрудник: {employee['full_name']}.\nТеперь выберите тип отсутствия.",
        reply_markup=leave_type_keyboard()
    )
    return config.LEAVE_GET_TYPE

async def admin_add_leave_get_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 3: Получает тип, просит ввести период."""
    leave_type = update.message.text
    if leave_type not in (config.BUTTON_LEAVE_TYPE_VACATION, config.BUTTON_LEAVE_TYPE_SICK):
        await update.message.reply_text("Пожалуйста, используйте кнопки для выбора типа.")
        return config.LEAVE_GET_TYPE
    
    context.user_data['leave_type'] = leave_type

    # --> ИЗМЕНЕНИЕ: Добавлено экранирование точек с помощью двойного слэша \\.
    text = f"Тип: {leave_type}\\. Теперь введите период в формате `ДД\\.ММ\\.ГГГГ-ДД\\.ММ\\.ГГГГ`\\."
    
    await update.message.reply_text(
        text=text,
        reply_markup=ReplyKeyboardMarkup([[config.BUTTON_ADMIN_BACK]], resize_keyboard=True),
        parse_mode='MarkdownV2'
    )
    return config.LEAVE_GET_PERIOD

async def admin_add_leave_get_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 4: Получает период, сохраняет данные и завершает."""
    try:
        start_date_str, end_date_str = update.message.text.split('-')
        start_date = datetime.strptime(start_date_str.strip(), '%d.%m.%Y').date()
        end_date = datetime.strptime(end_date_str.strip(), '%d.%m.%Y').date()
        
        if start_date > end_date:
            await update.message.reply_text("Ошибка: Начальная дата не может быть позже конечной.")
            return LEAVE_GET_PERIOD
            
    except (ValueError, IndexError):
        await update.message.reply_text("Неверный формат. Введите период как `ДД.ММ.ГГГГ-ДД.ММ.ГГГГ`.")
        return LEAVE_GET_PERIOD

    emp_id = context.user_data['leave_employee_id']
    emp_name = context.user_data['leave_employee_name']
    leave_type = context.user_data['leave_type']
    
    await database.add_leave_period(emp_id, start_date, end_date, leave_type)
    
    await update.message.reply_text(
        f"✅ Успешно! Для сотрудника {emp_name} назначен(а) {leave_type} с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}.",
        reply_markup=admin_menu_keyboard()
    )
    context.user_data.clear()
    return ADMIN_MENU

async def admin_cancel_leave_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 1: Просит переслать сообщение."""
    await update.message.reply_text(
        "Отмена отсутствия. Перешлите сообщение от сотрудника, для которого нужно отменить отпуск/больничный.",
        reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True)
    )
    return CANCEL_LEAVE_GET_ID

async def admin_cancel_leave_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 2: Получает ID, просит ввести период."""
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return config.CANCEL_LEAVE_GET_ID
        
    user_id = update.message.forward_origin.sender_user.id
    employee = await database.get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return config.CANCEL_LEAVE_GET_ID
    
    # ИСПРАВЛЕНИЕ: Используем 'full_name'
    context.user_data['cancel_leave_employee_id'] = user_id
    context.user_data['cancel_leave_employee_name'] = employee['full_name']
    
    # ИСПРАВЛЕНИЕ: Используем 'full_name' и экранируем точки
    text_to_send = (
        f"Выбран сотрудник: {employee['full_name']}\\.\n"
        f"Введите период для отмены отсутствия в формате `ДД\\.ММ\\.ГГГГ-ДД\\.ММ\\.ГГГГ`\\."
    )

    await update.message.reply_text(
        text=text_to_send,
        reply_markup=ReplyKeyboardMarkup([[config.BUTTON_ADMIN_BACK]], resize_keyboard=True),
        parse_mode='MarkdownV2'
    )
    return config.CANCEL_LEAVE_GET_PERIOD

async def admin_cancel_leave_get_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 3: Получает период, удаляет данные и завершает."""
    try:
        start_date_str, end_date_str = update.message.text.split('-')
        start_date = datetime.strptime(start_date_str.strip(), '%d.%m.%Y').date()
        end_date = datetime.strptime(end_date_str.strip(), '%d.%m.%Y').date()
        
        if start_date > end_date:
            await update.message.reply_text("Ошибка: Начальная дата не может быть позже конечной.")
            return CANCEL_LEAVE_GET_PERIOD
            
    except (ValueError, IndexError):
        await update.message.reply_text("Неверный формат. Введите период как `ДД.ММ.ГГГГ-ДД.ММ.ГГГГ`.")
        return CANCEL_LEAVE_GET_PERIOD

    emp_id = context.user_data['cancel_leave_employee_id']
    emp_name = context.user_data['cancel_leave_employee_name']
    
    rows_deleted = await database.cancel_leave_period(emp_id, start_date, end_date)
    
    await update.message.reply_text(
        f"✅ Успешно! Для сотрудника {emp_name} отменены все записи об отпусках/больничных с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}.\n"
        f"(Затронуто записей: {rows_deleted})",
        reply_markup=admin_menu_keyboard()
    )
    context.user_data.clear()
    return ADMIN_MENU

async def admin_back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_back_to_menu из bot.py)
    context.user_data.clear()
    await admin_command(update, context)
    return ADMIN_MENU

async def admin_web_ui(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет кнопку для открытия веб-интерфейса администратора."""
    # ВАЖНО: URL должен указывать на адрес, где запущен ваш webapp.
    # Для локального теста используется ngrok или аналоги для получения https-ссылки.
    # Для боевого сервера это будет ваш публичный домен.
    # Пока мы используем заглушку, которую вы замените на реальный URL.
    # ПРИМЕР: web_app_url = "https://your-domain.com"
    web_app_url = "https://panel.yolacloud.ru/" # ЗАМЕНИТЕ ЭТО НА ВАШ РЕАЛЬНЫЙ URL В БУДУЩЕМ

    keyboard = [[
        InlineKeyboardButton(
            "Открыть админ-панель",
            web_app=WebAppInfo(url=web_app_url)
        )
    ]]
    await update.message.reply_text(
        "Нажмите на кнопку ниже, чтобы открыть веб-интерфейс администратора.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )