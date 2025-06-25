# webapp.py
import logging
import urllib.parse
import hmac
import hashlib
import json
import config
import database
import re

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, field_validator
from typing import Dict, List, Optional

from datetime import date, time
from database import add_leave_period, cancel_leave_period

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

class LeaveRequest(BaseModel):
    employee_id: int
    leave_type: str
    start_date: date
    end_date: date


# --- НОВАЯ МОДЕЛЬ для добавления/редактирования сотрудника ---
class ScheduleData(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None

class EmployeeUpdateRequest(BaseModel):
    telegram_id: int
    full_name: str
    effective_date: date
    schedule: Dict[str, ScheduleData] # График будет словарем: "0" (Пн) -> {"start": "09:00", "end": "18:00"}

    @field_validator('schedule')
    def validate_schedule_times(cls, v):
        time_pattern = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
        for day, times in v.items():
            if times.start or times.end: # Если это не выходной
                if not (times.start and times.end and time_pattern.match(times.start) and time_pattern.match(times.end)):
                    raise ValueError(f"Неверный формат времени для дня {day}. Используйте ЧЧ:ММ.")
        return v
# --- КОНЕЦ НОВОЙ МОДЕЛИ ---

class Holiday(BaseModel):
    holiday_date: date
    holiday_name: str

class HolidayDeleteRequest(BaseModel):
    holiday_date: date

# --- Создание FastAPI приложения ---
app = FastAPI(title="Check-in Bot Admin Panel")

# --- API Эндпоинты (точки доступа к данным) ---

# --- НОВЫЕ ЭНДПОИНТЫ ДЛЯ ПРАЗДНИКОВ ---
@app.get("/api/holidays/{year}", response_model=List[Holiday])
async def get_holidays(year: int):
    """Возвращает список праздников за указанный год."""
    try:
        return await database.get_holidays_for_year(year)
    except Exception as e:
        logger.error(f"Ошибка при получении праздников за {year} год: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.post("/api/holidays/add")
async def add_new_holiday(request: Holiday):
    """Добавляет новый праздничный день."""
    try:
        await database.add_holiday(request.holiday_date, request.holiday_name)
        return {"status": "success", "message": "Праздник успешно добавлен."}
    except Exception as e:
        logger.error(f"Ошибка при добавлении праздника: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка сервера при добавлении праздника.")

@app.post("/api/holidays/delete")
async def delete_existing_holiday(request: HolidayDeleteRequest):
    """Удаляет праздничный день."""
    try:
        await database.delete_holiday(request.holiday_date)
        return {"status": "success", "message": "Праздник успешно удален."}
    except Exception as e:
        logger.error(f"Ошибка при удалении праздника: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка сервера при удалении праздника.")
# --- КОНЕЦ НОВЫХ ЭНДПОИНТОВ ---

# --- НОВЫЙ ЭНДПОИНТ ДЛЯ ПОЛУЧЕНИЯ ЛОГА ---
@app.get("/api/employees/{employee_id}/log", response_model=List[dict])
async def get_log_for_employee(employee_id: int, start_date: date, end_date: date):
    """Возвращает детализированный лог событий для сотрудника."""
    try:
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="Начальная дата не может быть позже конечной.")
        
        log_data = await database.get_employee_log(employee_id, start_date, end_date)
        return log_data
    except Exception as e:
        logger.error(f"Ошибка при получении лога для сотрудника {employee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")
# --- КОНЕЦ НОВОГО ЭНДПОИНТА ---


@app.get("/api/employees", response_model=List[Employee])
async def get_employees(q: Optional[str] = None, sort_by: Optional[str] = 'full_name', sort_order: Optional[str] = 'asc'):
    """
    Возвращает список всех активных сотрудников, используя database.py с поиском и сортировкой.
    """
    try:
        # Передаем параметры из запроса в нашу функцию для работы с БД
        db_employees = await database.get_all_active_employees(
            search_query=q, 
            sort_by=sort_by, 
            sort_order=sort_order
        )
        # Преобразуем ответ БД в модель Pydantic
        return [Employee(id=emp['telegram_id'], full_name=emp['full_name'], is_active=emp['is_active']) for emp in db_employees]
    except Exception as e:
        logger.error(f"Ошибка при получении списка сотрудников через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.get("/api/employees/{employee_id}")
async def get_employee_details(employee_id: int):
    """Возвращает детальную информацию о сотруднике, включая его последний график."""
    try:
        employee_data = await database.get_employee_with_schedule(employee_id)
        if not employee_data:
            raise HTTPException(status_code=404, detail="Сотрудник не найден")
        return employee_data
    except Exception as e:
        logger.error(f"Ошибка при получении деталей сотрудника {employee_id} через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

# --- НОВЫЙ ЭНДПОИНТ для обновления сотрудника ---
@app.post("/api/employees/update")
async def update_employee(request: EmployeeUpdateRequest):
    """Обновляет данные сотрудника и его график (создает новую версию)."""
    try:
        # Эта логика полностью аналогична добавлению, т.к. add_or_update_employee
        # обрабатывает и создание, и обновление. Мы просто вызываем ту же функцию.
        schedule_for_db = {}
        for day_index_str, times in request.schedule.items():
            day_index = int(day_index_str)
            if times.start and times.end:
                schedule_for_db[day_index] = {
                    "start": time.fromisoformat(times.start),
                    "end": time.fromisoformat(times.end)
                }
            else:
                schedule_for_db[day_index] = {}

        await database.add_or_update_employee(
            telegram_id=request.telegram_id,
            full_name=request.full_name,
            schedule_data=schedule_for_db,
            effective_date=request.effective_date
        )
        logger.info(f"Сотрудник {request.full_name} ({request.telegram_id}) обновлен через веб-интерфейс.")
        return {"status": "success", "message": "Данные сотрудника успешно обновлены."}
    except Exception as e:
        logger.error(f"Ошибка при обновлении сотрудника через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка сервера при обновлении: {e}")
# --- КОНЕЦ НОВЫХ ЭНДПОИНТОВ ---


# --- НОВЫЙ ЭНДПОИНТ для добавления сотрудника ---
@app.post("/api/employees/add")
async def add_employee(request: EmployeeUpdateRequest):
    """Добавляет нового сотрудника и его график."""
    try:
        # Преобразуем schedule из строк в объекты time для функции БД
        schedule_for_db = {}
        for day_index_str, times in request.schedule.items():
            day_index = int(day_index_str)
            if times.start and times.end:
                schedule_for_db[day_index] = {
                    "start": time.fromisoformat(times.start),
                    "end": time.fromisoformat(times.end)
                }
            else:
                schedule_for_db[day_index] = {} # Выходной

        await database.add_or_update_employee(
            telegram_id=request.telegram_id,
            full_name=request.full_name,
            schedule_data=schedule_for_db,
            effective_date=request.effective_date
        )
        logger.info(f"Сотрудник {request.full_name} ({request.telegram_id}) добавлен через веб-интерфейс.")
        return {"status": "success", "message": "Сотрудник успешно добавлен."}
    except Exception as e:
        logger.error(f"Ошибка при добавлении сотрудника через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка сервера при добавлении: {e}")
# --- КОНЕЦ НОВОГО ЭНДПОИНТА ---

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

@app.post("/api/leaves/add")
async def add_leave(request: LeaveRequest):
    """Назначает сотруднику период отсутствия."""
    try:
        await database.add_leave_period(
            telegram_id=request.employee_id,
            start_date=request.start_date,
            end_date=request.end_date,
            leave_type=request.leave_type
        )
        return {"status": "success", "message": "Период отсутствия успешно добавлен."}
    except Exception as e:
        logger.error(f"Ошибка при добавлении периода отсутствия через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {e}")

@app.post("/api/leaves/cancel")
async def cancel_leave(request: LeaveRequest):
    """Отменяет период отсутствия для сотрудника."""
    try:
        rows_deleted = await database.cancel_leave_period(
            telegram_id=request.employee_id,
            start_date=request.start_date,
            end_date=request.end_date
        )
        return {"status": "success", "message": f"Записи об отсутствии удалены ({rows_deleted} шт.)"}
    except Exception as e:
        logger.error(f"Ошибка при отмене периода отсутствия через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {e}")

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
