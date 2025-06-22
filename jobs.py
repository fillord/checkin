# jobs.py
import logging
from datetime import datetime, timedelta, time
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database import get_all_active_employees_with_schedules, has_checked_in_today, get_report_stats_for_period
from config import LOCAL_TIMEZONE, ADMIN_IDS, LIVENESS_ACTIONS # LIVENESS_ACTIONS - пример, если понадобится
import re
import config
import database

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

# jobs.py

def escape_markdown_v2(text: str) -> str:
    """Экранирует специальные символы для Telegram MarkdownV2."""
    # В MarkdownV2 нужно экранировать эти символы
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

# jobs.py

def _format_user_list(users_dict: dict, show_status=False) -> str:
    """Вспомогательная функция для красивого форматирования списка сотрудников."""
    if not users_dict:
        return "_пусто_"
    
    lines = []
    for data in users_dict.values():
        name = data['name'] if isinstance(data, dict) else data
        line = escape_markdown_v2(name)
        if show_status and isinstance(data, dict) and data.get('status'):
            status_text = {
                'LATE': 'опоздал(а)', 'VACATION': 'отпуск', 
                'SICK_LEAVE': 'больничный', 'APPROVED_LEAVE': 'отпросился(ась)'
            }.get(data['status'], '')
            if status_text:
                # --> ИЗМЕНЕНИЕ ЗДЕСЬ: Экранируем скобки
                line += f" *\\({escape_markdown_v2(status_text)}\\)*"
        lines.append(line)
    return "\n".join(f" \\- {line}" for line in lines)


async def send_dashboard_snapshot(context: ContextTypes.DEFAULT_TYPE, report_type: str):
    """Формирует и отправляет сводку-дашборд администраторам."""
    today = datetime.now(LOCAL_TIMEZONE).date()
    title_text = "📊 Дневная сводка" if report_type == 'midday' else "📊 Вечерняя сводка"
    logger.info(f"---[ЗАДАЧА]--- Формирование дашборда: {title_text} ---")

    stats = await database.get_dashboard_stats(today)

    # --> ИЗМЕНЕНИЕ: Экранируем все части заголовка перед сборкой
    title = escape_markdown_v2(title_text)
    date_str = escape_markdown_v2(today.strftime('%d.%m.%Y'))
    
    text_lines = [
        f"*{title} на {date_str}*",
        f"*Всего по графику:* {stats['total_scheduled']}\n",
    ]

    if report_type == 'midday':
        text_lines.extend([
            f"✅ *Пришли:* {len(stats['arrived'])}",
            _format_user_list(stats['arrived'], show_status=True), "\n",
            f"🌴 *Отсутствуют \\(уважит\\.\\):* {len(stats['on_leave'])}",
            _format_user_list(stats['on_leave'], show_status=True), "\n",
            f"❓ *Еще не отметились:* {len(stats['absent'])}",
            _format_user_list(stats['absent']),
        ])
    
    elif report_type == 'evening':
        on_site_or_incomplete = {**stats['incomplete'], **{k: v['name'] for k,v in stats.get('arrived', {}).items() if k not in stats.get('departed', {})}}
        absent_total = {**stats['absent'], **stats['incomplete']}

        text_lines.extend([
            f"🏁 *Завершили день \\(ушли\\):* {len(stats['departed'])}",
             _format_user_list(stats['departed']), "\n",
            f"🌴 *На больничном/в отпуске:* {len(stats['on_leave'])}",
            _format_user_list(stats['on_leave'], show_status=True), "\n",
            f"❌ *Прогул или не отметили уход:* {len(absent_total)}",
            _format_user_list(absent_total), "\n"
        ])

    final_text = "\n".join(text_lines)

    for admin_id in config.ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=final_text, parse_mode='MarkdownV2')
        except Exception as e:
            logger.error(f"Не удалось отправить дашборд админу {admin_id}: {e}")

# jobs.py
async def check_and_send_notifications(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(LOCAL_TIMEZONE)
    today_str = now.date().isoformat()

    # Инициализация и очистка данных
    if 'notifications_sent' not in context.bot_data:
        context.bot_data['notifications_sent'] = {}
    if 'unhandled_late_users' not in context.bot_data:
        context.bot_data['unhandled_late_users'] = set()
    if context.bot_data.get('last_cleanup_date') != today_str:
        context.bot_data['notifications_sent'] = {}
        context.bot_data['unhandled_late_users'] = set() # Очищаем и список опоздавших
        context.bot_data['last_cleanup_date'] = today_str

    employees = await database.get_all_active_employees_with_schedules(now.date())
    if not employees:
        return

    for emp_id, name, start_time_str in employees:
        try:
            if start_time_str is None:
                continue
            logger.info(f"---[ПРОВЕРКА]--- Сотрудник: {name} (ID: {emp_id}), график: '{start_time_str}'")
            # asyncpg возвращает объект time, а не строку. Преобразуем его.
            start_time = start_time_str if isinstance(start_time_str, time) else time.fromisoformat(start_time_str)
            shift_start_datetime = datetime.combine(now.date(), start_time, tzinfo=LOCAL_TIMEZONE)

            # --- Логика для напоминания (остается без изменений) ---
            warning_datetime = shift_start_datetime - timedelta(minutes=5)
            warning_key = f"{emp_id}_warning_{today_str}"
            if now >= warning_datetime and not context.bot_data['notifications_sent'].get(warning_key):
                if not await database.has_checked_in_today(emp_id, "ARRIVAL"):
                    await context.bot.send_message(chat_id=emp_id, text=f"🔔 Напоминание: ваш рабочий день скоро начнется. Пожалуйста, не забудьте отметиться.")
                context.bot_data['notifications_sent'][warning_key] = True

            # --- ИЗМЕНЕННАЯ Логика для уведомления об ОПОЗДАНИИ ---
            missed_datetime = shift_start_datetime + timedelta(minutes=5, seconds=30)
            missed_key = f"{emp_id}_missed_{today_str}"
            if now >= missed_datetime and not context.bot_data['notifications_sent'].get(missed_key):
                if not await database.has_checked_in_today(emp_id, "ARRIVAL"):
                    try:
                        # Просто отправляем текст и добавляем юзера в "список опоздавших"
                        await context.bot.send_message(chat_id=emp_id, text="Вы пропустили время для чек-ина. Пожалуйста, нажмите '✅ Приход', чтобы отметиться с опозданием.")
                        context.bot_data['unhandled_late_users'].add(emp_id)
                        logger.info(f"Сотрудник {name} (ID: {emp_id}) помечен как опоздавший.")
                    except Exception as e:
                        logger.error(f"ОШИБКА отправки уведомления об опоздании для {name}: {e}")
                context.bot_data['notifications_sent'][missed_key] = True
        except Exception as e:
            logger.error(f"Критическая ошибка в цикле уведомлений для сотрудника {name} (ID: {emp_id}): {e}", exc_info=True)

async def send_departure_reminders(context: ContextTypes.DEFAULT_TYPE):
    """
    Напоминает сотрудникам отметить уход через 15 минут после окончания ИХ смены.
    """
    now = datetime.now(LOCAL_TIMEZONE)
    today_str = now.date().isoformat()
    logger.info("---[ЗАДАЧА]--- Запуск проверки напоминаний об уходе ---")

    # Получаем всех, кто должен был работать сегодня
    employees = await database.get_all_active_employees_with_schedules(now.date())

    for emp_id, name, _ in employees:
        try:
            # Получаем полное расписание сотрудника на сегодня, включая время ухода
            schedule = await database.get_employee_today_schedule(emp_id)
            if not schedule or not schedule.get('end_time'):
                continue  # Пропускаем, если нет графика или времени ухода на сегодня

            # 1. Вычисляем персональное время напоминания
            shift_end_datetime = datetime.combine(now.date(), schedule['end_time'], tzinfo=LOCAL_TIMEZONE)
            reminder_datetime = shift_end_datetime + timedelta(minutes=15)

            # 2. Проверяем, наступило ли время напомнить именно этому сотруднику
            if now < reminder_datetime:
                continue # Еще рано, переходим к следующему сотруднику

            # 3. Проверяем, не отправляли ли мы ему уже напоминание сегодня
            reminder_key = f"{emp_id}_departure_reminder_{today_str}"
            if context.bot_data.get('notifications_sent', {}).get(reminder_key):
                continue # Уже напоминали, переходим к следующему

            # 4. Проверяем, отметил ли сотрудник приход и уход
            has_arrived = await database.has_checked_in_today(emp_id, "ARRIVAL")
            has_departed = await database.has_checked_in_today(emp_id, "DEPARTURE")

            # 5. Если он пришел, но еще не ушел - отправляем напоминание
            if has_arrived and not has_departed:
                await context.bot.send_message(
                    chat_id=emp_id,
                    text="👋 Не забудьте отметить уход! Это необходимо сделать до 23:00, иначе день будет отмечен как прогул."
                )
                logger.info(f"Отправлено напоминание об уходе сотруднику {name} (ID: {emp_id})")

            # 6. В любом случае помечаем, что мы его проверили, чтобы не спамить
            context.bot_data.setdefault('notifications_sent', {})[reminder_key] = True

        except Exception as e:
            logger.error(f"Ошибка в цикле напоминаний об уходе для {emp_id}: {e}", exc_info=True)


async def apply_incomplete_day_penalty(context: ContextTypes.DEFAULT_TYPE):
    """Применяет штраф за неотмеченный уход."""
    now = datetime.now(LOCAL_TIMEZONE)
    yesterday = now.date() - timedelta(days=1) # Задача запускается после полуночи за вчерашний день
    logger.info(f"---[ЗАДАЧА]--- Применение штрафов за неотмеченный уход за {yesterday.isoformat()} ---")

    employees = await database.get_all_active_employees_with_schedules(yesterday)

    for emp_id, name, _ in employees:
        try:
            # Проверяем чекины именно за вчерашний день
            has_arrived = await database.has_checked_in_on_date(emp_id, "ARRIVAL", yesterday)
            if not has_arrived:
                continue
            has_departed = await database.has_checked_in_on_date(emp_id, "DEPARTURE", yesterday)

            if has_arrived and not has_departed:
                await database.override_as_absent(emp_id, yesterday)
        except Exception as e:
            logger.error(f"Ошибка в цикле применения штрафов для {emp_id}: {e}", exc_info=True)