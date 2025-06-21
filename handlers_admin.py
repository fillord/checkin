# handlers_admin.py
import logging
import re
import csv
from datetime import datetime, date, timedelta
from io import StringIO, BytesIO

from telegram import Update, ReplyKeyboardMarkup, InputFile, MessageOriginUser, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes, ConversationHandler

import database
from jobs import send_report_for_period
from keyboards import admin_menu_keyboard, reports_menu_keyboard
from config import (
    ADMIN_IDS, ADMIN_MENU, ADMIN_REPORTS_MENU, REPORT_GET_DATES, MONTHLY_CSV_GET_MONTH,
    ADD_GET_ID, ADD_GET_NAME, MODIFY_GET_ID, DELETE_GET_ID, DELETE_CONFIRM,
    SCHEDULE_MON, DAYS_OF_WEEK, BUTTON_ADMIN_BACK, BUTTON_CONFIRM_DELETE, BUTTON_CANCEL_DELETE
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
    # ... (скопируйте сюда содержимое функции admin_monthly_csv_get_month из bot.py)
    try:
        month_str, year_str = update.message.text.strip().split('.')
        month, year = int(month_str), int(year_str)

        await update.message.reply_text(f"Пожалуйста, подождите, идет формирование сводного отчета за {month:02d}.{year}...")
        
        summary_data = await database.get_monthly_summary_data(year, month)

        if not summary_data or len(summary_data) <= 1:
            await update.message.reply_text("Нет данных для формирования отчета за указанный период.")
            await admin_reports_menu(update, context) 
            return ADMIN_REPORTS_MENU

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
        return ADMIN_REPORTS_MENU

    except (ValueError, IndexError):
        await update.message.reply_text(
            "Неверный формат. Пожалуйста, введите месяц и год как `ММ.ГГГГ` и попробуйте снова.",
            parse_mode='MarkdownV2'
        )
        return MONTHLY_CSV_GET_MONTH

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
    # ... (скопируйте сюда содержимое функции add_get_name из bot.py)
    context.user_data['new_employee_name'] = update.message.text
    await update.message.reply_text(f"Шаг 3: Настройка графика.\nВведите время для {DAYS_OF_WEEK[0]} (формат `09:00-18:00` или `0`).")
    return SCHEDULE_MON


async def admin_modify_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции admin_modify_start из bot.py)
    context.user_data.clear()
    await update.message.reply_text("Изменение графика. Перешлите сообщение от сотрудника.", reply_markup=ReplyKeyboardMarkup([[BUTTON_ADMIN_BACK]], resize_keyboard=True))
    return MODIFY_GET_ID


async def modify_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (скопируйте сюда содержимое функции modify_get_id из bot.py)
    if not isinstance(update.message.forward_origin, MessageOriginUser):
        await update.message.reply_text("Ошибка. Перешлите сообщение от реального пользователя.")
        return MODIFY_GET_ID
    user_id = update.message.forward_origin.sender_user.id
    employee = await database.get_employee_data(user_id, include_inactive=True)
    if not employee:
        await update.message.reply_text("Этот пользователь не найден в базе данных.")
        return MODIFY_GET_ID
    context.user_data['target_employee_id'] = user_id
    context.user_data['target_employee_name'] = employee['name']
    await update.message.reply_text(f"Изменение графика для: {employee['name']}.\nВведите новое время для {DAYS_OF_WEEK[0]} (`09:00-18:00` или `0`).")
    return SCHEDULE_MON


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
    await update.message.reply_text(f"Удалить сотрудника {employee['name']} ({user_id})? Это действие деактивирует его доступ.", reply_markup=ReplyKeyboardMarkup([[BUTTON_CONFIRM_DELETE, BUTTON_CANCEL_DELETE]], resize_keyboard=True))
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

def schedule_handler_factory(day_index: int):
    # ... (скопируйте сюда содержимое функции schedule_handler_factory из bot.py)
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
                await database.add_or_update_employee(telegram_id, full_name, schedule_data)
                
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
    # ... (скопируйте сюда содержимое функции admin_back_to_menu из bot.py)
    context.user_data.clear()
    await admin_command(update, context)
    return ADMIN_MENU