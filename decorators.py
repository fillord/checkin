# decorators.py
from functools import wraps
from telegram import Update, ReplyKeyboardRemove
from telegram.ext import ContextTypes, ConversationHandler

import database

def check_active_employee(func):
    """
    Декоратор, который проверяет, является ли пользователь активным сотрудником.
    Если нет - отправляет сообщение и завершает диалог.
    """
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return

        # Проверяем статус сотрудника в базе данных
        if not await database.is_employee_active(user.id):
            await update.message.reply_text(
                "Ваш аккаунт не найден в системе или был деактивирован. Пожалуйста, обратитесь к администратору.",
                reply_markup=ReplyKeyboardRemove()
            )
            # Завершаем диалог, чтобы пользователь не мог продолжать
            return ConversationHandler.END
        
        # Если сотрудник активен, выполняем основную функцию
        return await func(update, context, *args, **kwargs)
    return wrapper