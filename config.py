# config.py
import os
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# --- БАЗОВЫЕ НАСТРОЙКИ ---
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Не найден TELEGRAM_BOT_TOKEN в файле .env.")

# !!! ЗАМЕНИТЕ НА РЕАЛЬНЫЕ ID АДМИНОВ !!!
ADMIN_IDS = [1027958463]

# --- НАСТРОЙКИ ЛОГИКИ ---
LOCAL_TIMEZONE = ZoneInfo("Asia/Almaty")
WORK_LOCATION_COORDS = (43.25835460134987, 76.88279745482673)
ALLOWED_RADIUS_METERS = 200
FACE_DISTANCE_THRESHOLD = 0.6

# --- ФАЙЛЫ ---
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
MONTHLY_CSV_GET_MONTH = 19

# --- Тексты кнопок ---
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

# --- Другие константы ---
LIVENESS_ACTIONS = ["улыбнитесь в камеру", "покажите на камеру большой палец 👍", "покажите на камеру знак 'мир' двумя пальцами ✌️"]
DAYS_OF_WEEK = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]