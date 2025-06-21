# webapp.py
import logging
from fastapi import FastAPI, HTTPException 
from fastapi.responses import FileResponse
import aiosqlite
from pydantic import BaseModel
from typing import List
from database import get_monthly_summary_data
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
        if not report_data or len(report_data) <= 1: # Проверяем, есть ли данные кроме заголовка
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


# В будущем сюда можно добавлять другие эндпоинты:
# @app.get("/api/schedules/{employee_id}")
# @app.get("/api/reports/monthly/{year}/{month}")
# @app.post("/api/employees/update")
# и так далее...