import os
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Не найден TELEGRAM_BOT_TOKEN в файле .env.")

ADMIN_IDS = [1027958463]

LOCAL_TIMEZONE = ZoneInfo("Asia/Almaty")
WORK_LOCATION_COORDS = [
    (43.26102054257909, 76.89104192879864),
    (43.25840027467819, 76.88278342562465)
]
ALLOWED_RADIUS_METERS = 200
FACE_DISTANCE_THRESHOLD_CHECKIN = 0.6
FACE_DISTANCE_THRESHOLD_UPDATE = 0.75
DB_USER = os.getenv("DB_USER")
if not DB_USER:
    raise ValueError("Не найден DB_USER в файле .env.")
DB_PASSWORD = os.getenv("DB_PASSWORD")
if not DB_PASSWORD:
    raise ValueError("Не найден DB_PASSWORD в файле .env.")
DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise ValueError("Не найден DB_NAME в файле .env.")
DB_HOST = os.getenv("DB_HOST")
if not DB_HOST:
    raise ValueError("Не найден DB_HOST в файле .env.")
PERSISTENCE_FILE = "bot_persistence.pickle"

CHOOSE_ACTION, AWAITING_PHOTO, AWAITING_LOCATION, REGISTER_FACE = range(4)
(
    ADMIN_MENU, ADMIN_REPORTS_MENU,
    ADD_GET_ID, ADD_GET_NAME,
    MODIFY_GET_ID,
    DELETE_GET_ID, DELETE_CONFIRM,
    SCHEDULE_MON, SCHEDULE_TUE, SCHEDULE_WED, SCHEDULE_THU, SCHEDULE_FRI, SCHEDULE_SAT, SCHEDULE_SUN,
    REPORT_GET_DATES
) = range(4, 19)
MONTHLY_CSV_GET_MONTH = 19
AWAITING_LEAVE_REASON = 20
(
    LEAVE_GET_ID,
    LEAVE_GET_TYPE,
    LEAVE_GET_PERIOD
) = range(21, 24)
(
    CANCEL_LEAVE_GET_ID,
    CANCEL_LEAVE_GET_PERIOD
) = range(24, 26) 
SCHEDULE_GET_EFFECTIVE_DATE = 26
AWAITING_NEW_FACE_PHOTO = 27
(
    HOLIDAY_MENU,
    HOLIDAY_GET_ADD_DATE,
    HOLIDAY_GET_ADD_NAME,
    HOLIDAY_GET_DELETE_DATE
) = range(28, 32)
AWAITING_SCHEDULE_FILE = 32
AWAITING_ADD_EMPLOYEES_FILE = 33

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
BUTTON_REPORT_MONTHLY_CSV = "📅 Сводка за месяц в CSV"
BUTTON_CONFIRM_DELETE = "Да, удалить"
BUTTON_CANCEL_DELETE = "Нет, отмена"
BUTTON_ASK_LEAVE = "🙏 Отпроситься"
BUTTON_MANAGE_LEAVE = "🌴 Назначить отсутствие" 
BUTTON_LEAVE_TYPE_VACATION = "Отпуск"        
BUTTON_LEAVE_TYPE_SICK = "Больничный"
BUTTON_CANCEL_LEAVE = "🚫 Отменить отсутствие" 
BUTTON_UPDATE_PHOTO = "📸 Обновить фото" 
BUTTON_MANAGE_HOLIDAYS = "🎉 Управление праздниками"
BUTTON_MY_SCHEDULE = "📅 Мой график" 
BUTTON_CANCEL_ACTION = "❌ Отмена"
BUTTON_MY_STATS = "📊 Моя статистика" 

LIVENESS_ACTIONS = ["улыбнитесь в камеру", "покажите на камеру большой палец 👍", "покажите на камеру знак 'мир' двумя пальцами ✌️"]
DAYS_OF_WEEK = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

MAX_UPLOAD_FILE_SIZE_MB = 10