# main.py
import logging
import asyncio
import config
import database
import jobs
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    PicklePersistence,
    CallbackQueryHandler,
    AIORateLimiter
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from functools import partial
from config import (
    SCHEDULE_GET_EFFECTIVE_DATE,
    BUTTON_MY_SCHEDULE,
    BUTTON_CANCEL_ACTION,
    BUTTON_MY_STATS
)
from app_context import shutdown_executor
from keyboards import admin_menu_keyboard, reports_menu_keyboard
from handlers_user import (
    start_command, late_checkin_callback, handle_arrival, handle_departure,
    register_face, awaiting_photo, awaiting_location, employee_cancel_command,
    handle_late_checkin, ask_leave_start, ask_leave_get_reason, update_photo_start,
    update_photo_receive, get_personal_stats, show_my_schedule
)
from handlers_admin import (
    admin_command, admin_reports_menu, admin_get_today_report, admin_get_yesterday_report,
    admin_get_weekly_report, admin_custom_report_start, admin_custom_report_get_dates,
    admin_export_csv, admin_monthly_csv_start, admin_monthly_csv_get_month,
    admin_add_start, add_get_id, add_get_name, admin_modify_start, modify_get_id,
    admin_delete_start, delete_get_id, delete_confirm, schedule_handler_factory,
    admin_back_to_menu, handle_leave_request_decision, admin_add_leave_start, admin_add_leave_get_id,
    admin_add_leave_get_type, admin_add_leave_get_period, admin_cancel_leave_start, admin_cancel_leave_get_id, admin_cancel_leave_get_period,
    admin_web_ui, schedule_get_effective_date, admin_holidays_menu, holiday_add_start, holiday_get_add_date, holiday_get_add_name,
    holiday_delete_start, holiday_get_delete_date, bulk_update_start, handle_schedule_file, bulk_add_start, handle_add_employees_file
)

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

async def main() -> None:
    try:
        persistence = PicklePersistence(filepath=config.PERSISTENCE_FILE)
        rate_limiter = AIORateLimiter(max_retries=5)
        application = (
            Application.builder()
            .token(config.BOT_TOKEN)
            .persistence(persistence)
            .rate_limiter(rate_limiter)
            .build()
        )
        checkin_conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start_command),
                MessageHandler(filters.Regex(f"^{config.BUTTON_ASK_LEAVE}$"), ask_leave_start)
            ],
            states={
                config.CHOOSE_ACTION: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ARRIVAL}$"), handle_arrival),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_DEPARTURE}$"), handle_departure),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_UPDATE_PHOTO}$"), update_photo_start),
                    MessageHandler(filters.Regex(f"^{BUTTON_MY_SCHEDULE}$"), show_my_schedule),
                    MessageHandler(filters.Regex(f"^{BUTTON_MY_STATS}$"), get_personal_stats),
                ],
                config.AWAITING_LEAVE_REASON: [
                    MessageHandler(filters.Regex(f"^{BUTTON_CANCEL_ACTION}$"), employee_cancel_command),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, ask_leave_get_reason)
                ],
                config.REGISTER_FACE: [
                    MessageHandler(filters.PHOTO, register_face)
                ],
                config.AWAITING_NEW_FACE_PHOTO: [
                    MessageHandler(filters.Regex(f"^{BUTTON_CANCEL_ACTION}$"), employee_cancel_command),
                    MessageHandler(filters.PHOTO, update_photo_receive)
                ],
                config.AWAITING_PHOTO: [
                    MessageHandler(filters.Regex(f"^{BUTTON_CANCEL_ACTION}$"), employee_cancel_command),
                    MessageHandler(filters.PHOTO, awaiting_photo)
                ],
                config.AWAITING_LOCATION: [
                    MessageHandler(filters.Regex(f"^{BUTTON_CANCEL_ACTION}$"), employee_cancel_command),
                    MessageHandler(filters.LOCATION, awaiting_location)
                ],
            },
            fallbacks=[CommandHandler("cancel", employee_cancel_command)],
            allow_reentry=True, name="checkin_conversation", persistent=True,
        )
        schedule_handlers = [
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                (lambda day_index=i: schedule_handler_factory(day_index))()
            ) for i in range(7)
        ]
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
                    MessageHandler(filters.Regex(f"^{config.BUTTON_MANAGE_HOLIDAYS}$"), admin_holidays_menu),
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
                config.ADD_GET_NAME: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, add_get_name)
                ],
                config.MODIFY_GET_ID: [MessageHandler(filters.FORWARDED, modify_get_id)],
                config.DELETE_GET_ID: [MessageHandler(filters.FORWARDED, delete_get_id)],
                config.DELETE_CONFIRM: [MessageHandler(filters.Regex(f"^{config.BUTTON_CONFIRM_DELETE}$") | filters.Regex(f"^{config.BUTTON_CANCEL_DELETE}$"), delete_confirm)],
                config.LEAVE_GET_ID: [MessageHandler(filters.FORWARDED, admin_add_leave_get_id)],
                config.LEAVE_GET_TYPE: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_leave_get_type)
                ],
                config.LEAVE_GET_PERIOD: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_reports_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_leave_get_period)
                ],
                config.CANCEL_LEAVE_GET_ID: [MessageHandler(filters.FORWARDED, admin_cancel_leave_get_id)],
                config.CANCEL_LEAVE_GET_PERIOD: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, admin_cancel_leave_get_period)
                ],
                config.SCHEDULE_GET_EFFECTIVE_DATE: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_get_effective_date)
                ],
                config.SCHEDULE_MON: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[0]],
                config.SCHEDULE_TUE: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[1]],
                config.SCHEDULE_WED: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[2]],
                config.SCHEDULE_THU: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[3]],
                config.SCHEDULE_FRI: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[4]],
                config.SCHEDULE_SAT: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[5]],
                config.SCHEDULE_SUN: [MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), schedule_handlers[6]],
                config.HOLIDAY_MENU: [
                    MessageHandler(filters.Regex("^➕ Добавить праздник$"), holiday_add_start),
                    MessageHandler(filters.Regex("^➖ Удалить праздник$"), holiday_delete_start),
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_command),
                ],
                config.HOLIDAY_GET_ADD_DATE: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_holidays_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, holiday_get_add_date)
                ],
                config.HOLIDAY_GET_ADD_NAME: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), holiday_add_start),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, holiday_get_add_name)
                ],
                config.HOLIDAY_GET_DELETE_DATE: [
                    MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_holidays_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, holiday_get_delete_date)
                ]
            },
            fallbacks=[MessageHandler(filters.Regex(f"^{config.BUTTON_ADMIN_BACK}$"), admin_back_to_menu), CommandHandler("cancel", admin_back_to_menu)],
            name="admin_conversation", persistent=True,
        )
        bulk_update_conv = ConversationHandler(
            entry_points=[CommandHandler("bulk_update", bulk_update_start)],
            states={
                config.AWAITING_SCHEDULE_FILE: [MessageHandler(filters.Document.ALL, handle_schedule_file)]
            },
            fallbacks=[CommandHandler("cancel", admin_back_to_menu)]
        )
        bulk_add_conv = ConversationHandler(
            entry_points=[CommandHandler("bulk_add", bulk_add_start)],
            states={
                config.AWAITING_ADD_EMPLOYEES_FILE: [
                    MessageHandler(filters.Document.ALL, handle_add_employees_file)
                ]
            },
            fallbacks=[CommandHandler("cancel", admin_back_to_menu)]
        )
        application.add_handler(bulk_add_conv)
        application.add_handler(bulk_update_conv)
        application.add_handler(admin_conv_handler)
        application.add_handler(checkin_conv_handler)
        application.add_handler(CommandHandler("mystats", get_personal_stats))
        application.add_handler(CallbackQueryHandler(handle_leave_request_decision, pattern="^leave:"))
        application.add_handler(CommandHandler("web", admin_web_ui))
        scheduler = AsyncIOScheduler(timezone=config.LOCAL_TIMEZONE)
        scheduler.add_job(jobs.check_and_send_notifications, 'interval', minutes=1, args=[application])
        scheduler.add_job(jobs.send_daily_report_job, 'cron', hour=21, minute=0, args=[application])
        scheduler.add_job(jobs.send_departure_reminders, 'cron', hour='*', minute='*/5', args=[application])
        scheduler.add_job(jobs.apply_incomplete_day_penalty, 'cron', hour=0, minute=5, args=[application])
        scheduler.add_job(jobs.send_dashboard_snapshot, 'cron', hour=14, minute=35, args=[application, 'midday'])
        scheduler.add_job(jobs.send_dashboard_snapshot, 'cron', hour=20, minute=00, args=[application, 'evening'])
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