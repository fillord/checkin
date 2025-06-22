# webapp.py
import logging

import hmac
import hashlib
import urllib.parse

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
import aiosqlite
from pydantic import BaseModel
from typing import List
from database import get_monthly_summary_data, set_employee_active_status
# Импортируем наши настройки
import config

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Pydantic модели для валидации данных ---
# Это гарантирует, что наш API всегда возвращает данные в правильном формате
class Employee(BaseModel):
    id: int
    full_name: str
    is_active: bool

# --- Создание FastAPI приложения ---
app = FastAPI(title="Check-in Bot Admin Panel")


# --- API Эндпоинты (точки доступа к данным) ---

@app.get("/api/employees", response_model=List[Employee])
async def get_employees():
    """Возвращает список всех активных сотрудников."""
    employees = []
    try:
        async with aiosqlite.connect(config.DB_NAME) as db:
            cursor = await db.execute("SELECT telegram_id, full_name, is_active FROM employees WHERE is_active = TRUE ORDER BY full_name")
            rows = await cursor.fetchall()
            for row in rows:
                employees.append(Employee(id=row[0], full_name=row[1], is_active=row[2]))
    except Exception as e:
        logger.error(f"Ошибка при получении списка сотрудников: {e}")
    return employees

@app.get("/api/reports/monthly/{year}/{month}")
async def get_monthly_report(year: int, month: int):
    """Возвращает данные для сводного отчета за месяц."""
    try:
        # Используем нашу существующую функцию из database.py
        report_data = await get_monthly_summary_data(year, month)
        if not report_data or len(report_data) <= 1:
            raise HTTPException(status_code=404, detail="Нет данных за указанный период")

        # Возвращаем данные в формате JSON
        return {"data": report_data}
    except Exception as e:
        logger.error(f"Ошибка при формировании месячного отчета через API: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

# --- Эндпоинт для отдачи главной HTML страницы ---
@app.get("/")
async def read_root():
    """Отдает главную HTML страницу нашего веб-интерфейса."""
    return FileResponse('index.html')

class DeactivateRequest(BaseModel):
    id: int

@app.post("/api/employees/deactivate")
async def deactivate_employee(request: DeactivateRequest):
    """
    Деактивирует сотрудника по его ID.
    """
    try:
        # Вызываем нашу существующую функцию из database.py
        await set_employee_active_status(request.id, is_active=False)
        logger.info(f"Сотрудник с ID {request.id} был деактивирован через веб-интерфейс.")
        return {"status": "success", "message": f"Employee {request.id} deactivated."}
    except Exception as e:
        logger.error(f"Ошибка при деактивации сотрудника {request.id} через API: {e}")
        raise HTTPException(status_code=500, detail="Ошибка на сервере при деактивации сотрудника.")
    
class AuthRequest(BaseModel):
    initData: str


@app.post("/api/validate_user")
async def validate_user(request: AuthRequest):
    """Проверяет подлинность данных, полученных от Telegram Web App."""
    init_data = request.initData

    # Пытаемся извлечь хэш из данных
    try:
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
        hash_from_telegram = parsed_data.pop('hash', '')
        if not hash_from_telegram:
            raise ValueError("Хэш отсутствует в initData")
    except Exception:
        raise HTTPException(status_code=400, detail="Некорректные данные initData")

    # Формируем строку для проверки хэша
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))

    # Генерируем секретный ключ из токена бота
    secret_key = hmac.new("WebAppData".encode(), config.BOT_TOKEN.encode(), hashlib.sha256).digest()

    # Считаем наш хэш
    calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    # Сравниваем хэши
    if calculated_hash != hash_from_telegram:
        raise HTTPException(status_code=403, detail="Проверка данных не пройдена. Попытка подделки?")

    # Проверяем, что ID пользователя есть в списке админов
    user_info = urllib.parse.unquote(parsed_data.get('user', '{}'))
    import json
    user_id = json.loads(user_info).get('id')

    if user_id not in config.ADMIN_IDS:
        raise HTTPException(status_code=403, detail="Доступ запрещен. Вы не администратор.")

    logger.info(f"Пользователь {user_id} успешно прошел авторизацию в веб-панели.")
    return {"status": "ok", "user_id": user_id}

# В будущем сюда можно добавлять другие эндпоинты:
# @app.get("/api/schedules/{employee_id}")
# @app.get("/api/reports/monthly/{year}/{month}")
# @app.post("/api/employees/update")
# и так далее...