# keyboards.py
from telegram import ReplyKeyboardMarkup
from config import (
    BUTTON_ARRIVAL, BUTTON_DEPARTURE, BUTTON_ADMIN_ADD, BUTTON_ADMIN_MODIFY,
    BUTTON_ADMIN_DELETE, BUTTON_ADMIN_REPORTS, BUTTON_REPORT_TODAY,
    BUTTON_REPORT_YESTERDAY, BUTTON_REPORT_WEEK, BUTTON_REPORT_CUSTOM,
    BUTTON_REPORT_EXPORT, BUTTON_REPORT_MONTHLY_CSV, BUTTON_ADMIN_BACK
)

def main_menu_keyboard():
    return ReplyKeyboardMarkup([[BUTTON_ARRIVAL, BUTTON_DEPARTURE]], resize_keyboard=True)

def admin_menu_keyboard():
    return ReplyKeyboardMarkup([
        [BUTTON_ADMIN_ADD],
        [BUTTON_ADMIN_MODIFY, BUTTON_ADMIN_DELETE],
        [BUTTON_ADMIN_REPORTS]
    ], resize_keyboard=True)

def reports_menu_keyboard():
    return ReplyKeyboardMarkup([
        [BUTTON_REPORT_TODAY, BUTTON_REPORT_YESTERDAY],
        [BUTTON_REPORT_WEEK, BUTTON_REPORT_CUSTOM],
        [BUTTON_REPORT_EXPORT, BUTTON_REPORT_MONTHLY_CSV],
        [BUTTON_ADMIN_BACK]
    ], resize_keyboard=True)