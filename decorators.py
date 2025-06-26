# decorators.py
from functools import wraps
from datetime import datetime, timedelta
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

def user_level_cooldown(seconds: int):
    """
    Декоратор для установки персональной задержки (cooldown) на команду.
    Не позволяет пользователю вызывать одну и ту же команду чаще, чем раз в N секунд.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            now = datetime.now()
            
            # Создаем уникальный ключ для каждой функции, чтобы задержки не пересекались
            cooldown_key = f"cooldown_{func.__name__}_{user_id}"
            
            last_called = context.bot_data.get(cooldown_key, datetime.min)
            
            if now < last_called + timedelta(seconds=seconds):
                remaining = (last_called + timedelta(seconds=seconds) - now).seconds
                await update.message.reply_text(
                    f"Эту команду можно использовать не чаще одного раза в {seconds} секунд. "
                    f"Пожалуйста, подождите еще {remaining} сек."
                )
                return
            
            context.bot_data[cooldown_key] = now
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator