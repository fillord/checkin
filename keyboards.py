# keyboards.py
from telegram import ReplyKeyboardMarkup
from config import (
    BUTTON_ARRIVAL, BUTTON_DEPARTURE, BUTTON_ASK_LEAVE, BUTTON_ADMIN_ADD, BUTTON_ADMIN_MODIFY,
    BUTTON_ADMIN_DELETE, BUTTON_ADMIN_REPORTS, BUTTON_REPORT_TODAY,
    BUTTON_REPORT_YESTERDAY, BUTTON_REPORT_WEEK, BUTTON_REPORT_CUSTOM,
    BUTTON_REPORT_EXPORT, BUTTON_REPORT_MONTHLY_CSV, BUTTON_ADMIN_BACK,
    BUTTON_LEAVE_TYPE_VACATION, BUTTON_LEAVE_TYPE_SICK, BUTTON_MANAGE_LEAVE, BUTTON_CANCEL_LEAVE, BUTTON_UPDATE_PHOTO
)

def main_menu_keyboard():

    return ReplyKeyboardMarkup(
        [
            [BUTTON_ARRIVAL, BUTTON_DEPARTURE],
            [BUTTON_ASK_LEAVE, BUTTON_UPDATE_PHOTO]
        ],
        resize_keyboard=True
    )
def admin_menu_keyboard():
    return ReplyKeyboardMarkup([
        [BUTTON_ADMIN_ADD],
        [BUTTON_ADMIN_MODIFY, BUTTON_ADMIN_DELETE],
        [BUTTON_ADMIN_REPORTS,  BUTTON_MANAGE_LEAVE],
        [BUTTON_CANCEL_LEAVE]
    ], resize_keyboard=True)

def reports_menu_keyboard():
    return ReplyKeyboardMarkup([
        [BUTTON_REPORT_TODAY, BUTTON_REPORT_YESTERDAY],
        [BUTTON_REPORT_WEEK, BUTTON_REPORT_CUSTOM],
        [BUTTON_REPORT_EXPORT, BUTTON_REPORT_MONTHLY_CSV],
        [BUTTON_ADMIN_BACK]
    ], resize_keyboard=True)

def leave_type_keyboard():
    return ReplyKeyboardMarkup([
        [BUTTON_LEAVE_TYPE_VACATION, BUTTON_LEAVE_TYPE_SICK],
        [BUTTON_ADMIN_BACK]
    ], resize_keyboard=True)