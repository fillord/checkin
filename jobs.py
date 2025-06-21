# jobs.py
import logging
from datetime import datetime, timedelta, time
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database import get_all_active_employees_with_schedules, has_checked_in_today, get_report_stats_for_period
from config import LOCAL_TIMEZONE, ADMIN_IDS, LIVENESS_ACTIONS # LIVENESS_ACTIONS - пример, если понадобится
import re


logger = logging.getLogger(__name__)

async def send_report_for_period(start_date, end_date, context: ContextTypes.DEFAULT_TYPE, title_prefix: str, chat_ids: list[int] | int):
    # ... (скопируйте сюда содержимое функции send_report_for_period из bot.py)
    if not isinstance(chat_ids, list):
        chat_ids = [chat_ids]
    
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"Формирую отчет: {title_prefix}...")
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление о начале формирования отчета на {chat_id}: {e}")

    stats = await get_report_stats_for_period(start_date, end_date)
    
    def escape_markdown(text: str) -> str:
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

    start_str = escape_markdown(start_date.strftime('%d.%m.%Y'))
    end_str = escape_markdown(end_date.strftime('%d.%m.%Y'))
    period_str = start_str if start_date == end_date else f"с {start_str} по {end_str}"
    
    report_lines = [
        f"📊 *{escape_markdown(title_prefix)} за {period_str}*",
        "",
        f"👥 *Всего рабочих дней* \\(план\\): {stats['total_work_days']}",
        f"✅ *Всего приходов* \\(факт\\): {stats['total_arrivals']}",
        f"🕒 *Из них опозданий:* {stats['total_lates']}",
    ]
    
    if stats.get('late_employees'):
        for name, dates in stats['late_employees'].items():
            escaped_name = escape_markdown(name)
            escaped_dates = escape_markdown(', '.join(dates))
            report_lines.append(f"    `└` *{escaped_name}* \\({escaped_dates}\\)")

    report_lines.append("") 
    report_lines.append(f"❌ *Пропуски* \\({len(stats['absences'])} человек\\(а\\)\\):")
    
    if stats['absences']:
        for name, dates in stats['absences'].items():
            escaped_name = escape_markdown(name)
            escaped_dates = escape_markdown(', '.join(dates))
            report_lines.append(f"    `└` *{escaped_name}*: {escaped_dates}")
    else:
        report_lines.append(r"    `└` Пропусков нет\!")
    
    report_text = "\n".join(report_lines)
        
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=chat_id, text=report_text, parse_mode='MarkdownV2')
        except Exception as e:
            logger.error(f"Не удалось отправить отчет на {chat_id}: {e}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text=f"Критическая ошибка при отправке отчета: {e}")

async def send_daily_report_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Формирование и отправка автоматического дневного отчета...")
    await send_report_for_period(datetime.now(LOCAL_TIMEZONE).date(), datetime.now(LOCAL_TIMEZONE).date(), context, "Ежедневный отчет", ADMIN_IDS)

async def check_and_send_notifications(context: ContextTypes.DEFAULT_TYPE):
    # ... (скопируйте сюда содержимое функции check_and_send_notifications из bot.py)
    logger.info("---[ЗАДАЧА]--- Запуск проверки уведомлений ---")
    now = datetime.now(LOCAL_TIMEZONE)
    today_str = now.date().isoformat()

    if 'notifications_sent' not in context.bot_data:
        context.bot_data['notifications_sent'] = {}
    if context.bot_data.get('last_cleanup_date') != today_str:
        logger.info(f"---[ЗАДАЧА]--- Новый день ({today_str})! Очистка старых записей.")
        context.bot_data['notifications_sent'] = {}
        context.bot_data['last_cleanup_date'] = today_str

    employees = await get_all_active_employees_with_schedules(now.weekday())
    if not employees:
        return
        
    logger.info(f"---[ЗАДАЧА]--- Найдено {len(employees)} сотрудников с расписанием на сегодня.")

    for emp_id, name, start_time_str in employees:
        try:
            logger.info(f"---[ПРОВЕРКА]--- Сотрудник: {name} (ID: {emp_id}), график: '{start_time_str}'")
            start_time = time.fromisoformat(start_time_str)
            shift_start_datetime = datetime.combine(now.date(), start_time, tzinfo=LOCAL_TIMEZONE)

            warning_datetime = shift_start_datetime - timedelta(minutes=5)
            missed_datetime = shift_start_datetime + timedelta(minutes=5, seconds=30)
            
            logger.info(f"    [ДЕТАЛИ] Текущее время (now)  : {now.isoformat()}")
            logger.info(f"    [ДЕТАЛИ] Время предупреждения : {warning_datetime.isoformat()}")
            logger.info(f"    [ДЕТАЛИ] Время опоздания      : {missed_datetime.isoformat()}")
            
            warning_key = f"{emp_id}_warning_{today_str}"
            missed_key = f"{emp_id}_missed_{today_str}"
            
            is_time_for_warning = now >= warning_datetime
            is_warning_sent = context.bot_data['notifications_sent'].get(warning_key, False)
            logger.info(f"    [УСЛОВИЕ WARNING] now >= warning_datetime? -> {is_time_for_warning}. sent? -> {is_warning_sent}")

            is_time_for_missed = now >= missed_datetime
            is_missed_sent = context.bot_data['notifications_sent'].get(missed_key, False)
            logger.info(f"    [УСЛОВИЕ MISSED]  now >= missed_datetime?  -> {is_time_for_missed}. sent? -> {is_missed_sent}")
            
            if is_time_for_warning and not is_warning_sent:
                has_checked_in = await has_checked_in_today(emp_id, "ARRIVAL")
                logger.info(f"    -> Проверка чекина для ПРЕДУПРЕЖДЕНИЯ: {'ЕСТЬ' if has_checked_in else 'НЕТ'}")
                if not has_checked_in:
                    await context.bot.send_message(chat_id=emp_id, text=f"🔔 Напоминание: ваш рабочий день скоро начнется. Пожалуйста, не забудьте отметиться.")
                    logger.info(f"    -> ОТПРАВЛЕНО ПРЕДУПРЕЖДЕНИЕ для {name}.")
                context.bot_data['notifications_sent'][warning_key] = True

            if is_time_for_missed and not is_missed_sent:
                has_checked_in = await has_checked_in_today(emp_id, "ARRIVAL")
                logger.info(f"    -> Проверка чекина для ОПОЗДАНИЯ: {'ЕСТЬ' if has_checked_in else 'НЕТ'}")
                if not has_checked_in:
                    keyboard = [[InlineKeyboardButton("Отметиться с опозданием", callback_data="late_checkin")]]
                    await context.bot.send_message(chat_id=emp_id, text="Вы пропустили время для чек-ина. Вы можете отметиться сейчас, но это будет зафиксировано как опоздание.", reply_markup=InlineKeyboardMarkup(keyboard))
                    logger.info(f"    -> ОТПРАВЛЕНО уведомление об ОПОЗДАНИИ для {name}.")
                context.bot_data['notifications_sent'][missed_key] = True
        
        except Exception as e:
            logger.error(f"---[КРИТИЧЕСКАЯ ОШИБКА]--- в цикле для сотрудника {name} (ID: {emp_id}): {e}", exc_info=True)