# main.py
import logging
import asyncio
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    PicklePersistence,
    CallbackQueryHandler
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
# concurrent.futures.ProcessPoolExecutor больше не нужен здесь напрямую

# Импортируем наши модули
import config
import database
import jobs
from app_context import shutdown_executor
from keyboards import admin_menu_keyboard, reports_menu_keyboard
from handlers_user import (
    start_command, late_checkin_callback, handle_arrival, handle_departure,
    register_face, awaiting_photo, awaiting_location, employee_cancel_command,
    handle_late_checkin, ask_leave_start, ask_leave_get_reason
)
from handlers_admin import (
    admin_command, admin_reports_menu, admin_get_today_report, admin_get_yesterday_report,
    admin_get_weekly_report, admin_custom_report_start, admin_custom_report_get_dates,
    admin_export_csv, admin_monthly_csv_start, admin_monthly_csv_get_month,
    admin_add_start, add_get_id, add_get_name, admin_modify_start, modify_get_id,
    admin_delete_start, delete_get_id, delete_confirm, schedule_handler_factory,
    admin_back_to_menu, handle_leave_request_decision, admin_add_leave_start, admin_add_leave_get_id,
    admin_add_leave_get_type, admin_add_leave_get_period, admin_cancel_leave_start, admin_cancel_leave_get_id, admin_cancel_leave_get_period,
)

# Настройка логирования
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

async def main() -> None:
    """Основная функция для запуска бота."""
    
    # Блок try...finally для гарантированного закрытия пула
    try:
        persistence = PicklePersistence(filepath=config.PERSISTENCE_FILE)
        application = Application.builder().token(config.BOT_TOKEN).persistence(persistence).build()
        
        # --- РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ (без изменений) ---
        checkin_conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start_command),
                # УДАЛЯЕМ СТАРУЮ ТОЧКУ ВХОДА ДЛЯ ИНЛАЙН-КНОПКИ
                # CallbackQueryHandler(late_checkin_callback, pattern="^late_checkin$")
                MessageHandler(filters.Regex(f"^{config.BUTTON_ASK_LEAVE}$"), ask_leave_start) 
            ],
            states={
                config.CHOOSE_ACTION: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ARRIVAL}$"), handle_arrival), 
                    MessageHandler(filters.Regex(f"^{config.BUTTON_DEPARTURE}$"), handle_departure)
                ],
                config.AWAITING_LEAVE_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_leave_get_reason)],
                config.REGISTER_FACE: [MessageHandler(filters.PHOTO, register_face)],
                config.AWAITING_PHOTO: [MessageHandler(filters.PHOTO, awaiting_photo)],
                config.AWAITING_LOCATION: [MessageHandler(filters.LOCATION, awaiting_location)],
            },
            fallbacks=[CommandHandler("cancel", employee_cancel_command)],
            allow_reentry=True, name="checkin_conversation", persistent=True,
        )
        
        schedule_handlers = [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_handler_factory(i)) for i in range(7)]
        admin_conv_handler = ConversationHandler(
            entry_points=[CommandHandler("admin", admin_command)],
            states={
                config.ADMIN_MENU: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_ADD}$"), admin_add_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_MODIFY}$"), admin_modify_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_DELETE}$"), admin_delete_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_REPORTS}$"), admin_reports_menu),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_MANAGE_LEAVE}$"), admin_add_leave_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_CANCEL_LEAVE}$"), admin_cancel_leave_start),
                ],
                config.ADMIN_REPORTS_MENU: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_REPORT_TODAY}$"), admin_get_today_report),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_REPORT_YESTERDAY}$"), admin_get_yesterday_report),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_REPORT_WEEK}$"), admin_get_weekly_report),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_REPORT_CUSTOM}$"), admin_custom_report_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_REPORT_EXPORT}$"), admin_export_csv),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_REPORT_MONTHLY_CSV}$"), admin_monthly_csv_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_command),
                ],
                config.REPORT_GET_DATES: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_reports_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_custom_report_get_dates)
                ],
                config.MONTHLY_CSV_GET_MONTH: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_reports_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_monthly_csv_get_month)
                ],
                config.ADD_GET_ID: [MessageHandler(filters.FORWARDED, add_get_id)],
                config.ADD_GET_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_get_name)],
                config.MODIFY_GET_ID: [MessageHandler(filters.FORWARDED, modify_get_id)],
                config.DELETE_GET_ID: [MessageHandler(filters.FORWARDED, delete_get_id)],
                config.DELETE_CONFIRM: [MessageHandler(filters.Regex(f"^{config.BUTTON_CONFIRM_DELETE}$") | filters.Regex(f"^{config.BUTTON_CANCEL_DELETE}$"), delete_confirm)],
                config.SCHEDULE_MON: [schedule_handlers[0]], config.SCHEDULE_TUE: [schedule_handlers[1]], config.SCHEDULE_WED: [schedule_handlers[2]],
                config.SCHEDULE_THU: [schedule_handlers[3]], config.SCHEDULE_FRI: [schedule_handlers[4]], config.SCHEDULE_SAT: [schedule_handlers[5]],
                config.SCHEDULE_SUN: [schedule_handlers[6]],
                config.LEAVE_GET_ID: [MessageHandler(filters.FORWARDED, admin_add_leave_get_id)],
                config.LEAVE_GET_TYPE: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_leave_get_type)
                ],
                config.LEAVE_GET_PERIOD: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_reports_menu), # <-- ИЗМЕНЕНИЕ
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_leave_get_period)
                ],
                config.CANCEL_LEAVE_GET_ID: [MessageHandler(filters.FORWARDED, admin_cancel_leave_get_id)],
                config.CANCEL_LEAVE_GET_PERIOD: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_cancel_leave_get_period)
                ],
            },
            fallbacks=[MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), CommandHandler("cancel", admin_back_to_menu)],
            name="admin_conversation", persistent=True,
        )

        application.add_handler(admin_conv_handler)
        application.add_handler(checkin_conv_handler)
        
        # Добавляем отдельный обработчик для решения админа по уходу
        application.add_handler(CallbackQueryHandler(handle_leave_request_decision, pattern="^leave:"))

        scheduler = AsyncIOScheduler(timezone=config.LOCAL_TIMEZONE)
        scheduler.add_job(jobs.check_and_send_notifications, 'interval', minutes=1, args=[application])
        scheduler.add_job(jobs.send_daily_report_job, 'cron', hour=21, minute=0, args=[application])
        
        # Запускаем проверку каждые 15 минут в течение всего дня, чтобы охватить любой график
        scheduler.add_job(jobs.send_departure_reminders, 'cron', hour='*', minute='*/5', args=[application])
        scheduler.add_job(jobs.apply_incomplete_day_penalty, 'cron', hour=0, minute=5, args=[application]) # Применяем штраф в 00:05 за вчерашний день
    
        async with application:
            await database.init_db()
            await application.initialize()
            await application.updater.start_polling()
            await application.start()
            scheduler.start()
            logger.info("Бот и планировщик запущены. Нажмите Ctrl+C для остановки.")
            await asyncio.Event().wait()
            
    finally:
        logger.info("Закрытие пула процессов...")
        shutdown_executor()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске: {e}", exc_info=True)