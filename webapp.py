# webapp.py
import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List
import urllib.parse
import hmac
import hashlib
import json

# Импортируем наши модули
import config
import database # <-- Теперь мы импортируем наш основной модуль для работы с БД

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Pydantic модели для валидации данных ---
class Employee(BaseModel):
    id: int
    full_name: str
    is_active: bool

class DeactivateRequest(BaseModel):
    id: int

class AuthRequest(BaseModel):
    initData: str

# --- Создание FastAPI приложения ---
app = FastAPI(title="Check-in Bot Admin Panel")


# --- API Эндпоинты (точки доступа к данным) ---

@app.get("/api/employees", response_model=List[Employee])
async def get_employees():
    """Возвращает список всех активных сотрудников, используя database.py."""
    try:
        # Вызываем функцию из нашего модуля, больше никакой логики БД здесь нет
        db_employees = await database.get_all_active_employees()
        # Преобразуем ответ БД в модель Pydantic
        return [Employee(id=emp['telegram_id'], full_name=emp['full_name'], is_active=emp['is_active']) for emp in db_employees]
    except Exception as e:
        logger.error(f"Ошибка при получении списка сотрудников через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.post("/api/employees/deactivate")
async def deactivate_employee(request: DeactivateRequest):
    """Деактивирует сотрудника по его ID, используя database.py."""
    try:
        await database.set_employee_active_status(request.id, is_active=False)
        logger.info(f"Сотрудник с ID {request.id} был деактивирован через веб-интерфейс.")
        return {"status": "success", "message": f"Employee {request.id} deactivated."}
    except Exception as e:
        logger.error(f"Ошибка при деактивации сотрудника {request.id} через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка на сервере при деактивации сотрудника.")


@app.get("/api/reports/monthly/{year}/{month}")
async def get_monthly_report(year: int, month: int):
    """Возвращает данные для сводного отчета за месяц, используя database.py."""
    try:
        report_data = await database.get_monthly_summary_data(year, month)
        if not report_data or len(report_data) <= 1:
            raise HTTPException(status_code=404, detail="Нет данных за указанный период")
        return {"data": report_data}
    except Exception as e:
        logger.error(f"Ошибка при формировании месячного отчета через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.post("/api/validate_user")
async def validate_user(request: AuthRequest):
    """Проверяет подлинность данных, полученных от Telegram Web App."""
    try:
        init_data = request.initData
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
        hash_from_telegram = parsed_data.pop('hash', '')
        if not hash_from_telegram:
            raise ValueError("Хэш отсутствует в initData")
        
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new("WebAppData".encode(), config.BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if calculated_hash != hash_from_telegram:
            raise HTTPException(status_code=403, detail="Проверка данных не пройдена.")
        
        user_info = json.loads(urllib.parse.unquote(parsed_data.get('user', '{}')))
        user_id = user_info.get('id')
        if user_id not in config.ADMIN_IDS:
            raise HTTPException(status_code=403, detail="Доступ запрещен.")
            
        logger.info(f"Пользователь {user_id} успешно прошел авторизацию в веб-панели.")
        return {"status": "ok", "user_id": user_id}
    except Exception as e:
        logger.error(f"Ошибка валидации пользователя: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail="Некорректные данные для авторизации.")


# --- Эндпоинт для отдачи главной HTML страницы ---
@app.get("/")
async def read_root():
    """Отдает главную HTML страницу нашего веб-интерфейса."""
    return FileResponse('index.html')