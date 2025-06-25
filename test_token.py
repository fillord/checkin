# test_token.py
import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем токен
token = os.getenv("TELEGRAM_BOT_TOKEN")

if token:
    print("Токен успешно загружен из .env файла.")
    # Выводим только первые и последние символы, чтобы не светить токен целиком
    print(f"Первые 8 символов: {token[:8]}")
    print(f"Последние 8 символов: {token[-8:]}")
else:
    print("ОШИБКА: Токен не найден в .env файле!")