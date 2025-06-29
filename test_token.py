import os
from dotenv import load_dotenv
load_dotenv()
token = os.getenv("TELEGRAM_BOT_TOKEN")

if token:
    print("Токен успешно загружен из .env файла.")
    print(f"Первые 8 символов: {token[:8]}")
    print(f"Последние 8 символов: {token[-8:]}")
else:
    print("ОШИБКА: Токен не найден в .env файле!")