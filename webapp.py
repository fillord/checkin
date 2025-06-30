import logging
import urllib.parse
import hmac
import hashlib
import json
import config
import database
import re
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel, field_validator
from typing import Dict, List, Optional, Annotated
from datetime import date, time
from database import add_leave_period, cancel_leave_period

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

class ReplacementRequest(BaseModel):
    original_employee_id: int
    substitute_employee_id: int
    start_date: date
    end_date: date

class ScheduleData(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None

class EmployeeUpdateRequest(BaseModel):
    telegram_id: int
    full_name: str
    effective_date: date
    schedule: Dict[str, ScheduleData]

    @field_validator('schedule')
    def validate_schedule_times(cls, v):
        time_pattern = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
        for day, times in v.items():
            if times.start or times.end:
                if not (times.start and times.end and time_pattern.match(times.start) and time_pattern.match(times.end)):
                    raise ValueError(f"Неверный формат времени для дня {day}. Используйте ЧЧ:ММ.")
        return v

class Holiday(BaseModel):
    holiday_date: date
    holiday_name: str

class HolidayDeleteRequest(BaseModel):
    holiday_date: date

class CancelReplacementRequest(BaseModel):
    leave_id: int

async def get_validated_user(x_telegram_init_data: Annotated[str, Header()]) -> dict:
    try:
        init_data = x_telegram_init_data
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
        hash_from_telegram = parsed_data.pop('hash', '')
        if not hash_from_telegram:
            raise ValueError("Хэш отсутствует в initData")
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new("WebAppData".encode(), config.BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calculated_hash, hash_from_telegram):
            logger.warning(f"Проверка данных не пройдена. Хэш не совпал.")
            raise HTTPException(status_code=403, detail="Проверка данных не пройдена.")
        user_info = json.loads(urllib.parse.unquote(parsed_data.get('user', '{}')))
        user_id = user_info.get('id')
        if user_id not in config.ADMIN_IDS:
            raise HTTPException(status_code=403, detail="Доступ запрещен.")
        return user_info
    except (ValueError, KeyError, json.JSONDecodeError) as e:
        logger.error(f"Критическая ошибка валидации пользователя: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail="Некорректные данные для авторизации.")

app = FastAPI(title="Check-in Bot Admin Panel")

@app.get("/api/holidays/{year}", response_model=List[Holiday])
async def get_holidays(year: int, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        return await database.get_holidays_for_year(year)
    except Exception as e:
        logger.error(f"Ошибка при получении праздников за {year} год: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.post("/api/holidays/add")
async def add_new_holiday(request: Holiday, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        await database.add_holiday(request.holiday_date, request.holiday_name)
        return {"status": "success", "message": "Праздник успешно добавлен."}
    except Exception as e:
        logger.error(f"Ошибка при добавлении праздника: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка сервера при добавлении праздника.")

@app.post("/api/holidays/delete")
async def delete_existing_holiday(request: HolidayDeleteRequest, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        await database.delete_holiday(request.holiday_date)
        return {"status": "success", "message": "Праздник успешно удален."}
    except Exception as e:
        logger.error(f"Ошибка при удалении праздника: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка сервера при удалении праздника.")

@app.get("/api/employees/{employee_id}/log", response_model=List[dict])
async def get_log_for_employee(employee_id: int, start_date: date, end_date: date, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="Начальная дата не может быть позже конечной.")
        log_data = await database.get_employee_log(employee_id, start_date, end_date)
        return log_data
    except Exception as e:
        logger.error(f"Ошибка при получении лога для сотрудника {employee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.get("/api/employees", response_model=List[Employee])
async def get_employees(
    user: Annotated[dict, Depends(get_validated_user)],
    q: Optional[str] = None, 
    sort_by: Optional[str] = 'full_name', 
    sort_order: Optional[str] = 'asc'
):
    try:
        db_employees = await database.get_all_active_employees(
            search_query=q, 
            sort_by=sort_by, 
            sort_order=sort_order
        )
        return [Employee(id=emp['telegram_id'], full_name=emp['full_name'], is_active=emp['is_active']) for emp in db_employees]
    except Exception as e:
        logger.error(f"Ошибка при получении списка сотрудников через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.get("/api/employees/{employee_id}")
async def get_employee_details(employee_id: int, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        employee_data = await database.get_employee_with_schedule(employee_id)
        if not employee_data:
            raise HTTPException(status_code=404, detail="Сотрудник не найден")
        return employee_data
    except Exception as e:
        logger.error(f"Ошибка при получении деталей сотрудника {employee_id} через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.post("/api/employees/update")
async def update_employee(request: EmployeeUpdateRequest, user: Annotated[dict, Depends(get_validated_user)]):
    try:
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

@app.post("/api/employees/add")
async def add_employee(request: EmployeeUpdateRequest, user: Annotated[dict, Depends(get_validated_user)]):
    try:
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
        logger.info(f"Сотрудник {request.full_name} ({request.telegram_id}) добавлен через веб-интерфейс.")
        return {"status": "success", "message": "Сотрудник успешно добавлен."}
    except Exception as e:
        logger.error(f"Ошибка при добавлении сотрудника через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка сервера при добавлении: {e}")

@app.post("/api/employees/deactivate")
async def deactivate_employee(request: DeactivateRequest, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        await database.set_employee_active_status(request.id, is_active=False)
        logger.info(f"Сотрудник с ID {request.id} был деактивирован через веб-интерфейс.")
        return {"status": "success", "message": f"Employee {request.id} deactivated."}
    except Exception as e:
        logger.error(f"Ошибка при деактивации сотрудника {request.id} через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка на сервере при деактивации сотрудника.")

@app.post("/api/leaves/add")
async def add_leave(request: LeaveRequest, user: Annotated[dict, Depends(get_validated_user)]):
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
async def cancel_leave(request: LeaveRequest, user: Annotated[dict, Depends(get_validated_user)]):
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

@app.post("/api/employees/replacement")
async def create_replacement(request: ReplacementRequest, user: Annotated[dict, Depends(get_validated_user)]):
    """Создает временную замену сотрудника."""
    if request.original_employee_id == request.substitute_employee_id:
        raise HTTPException(status_code=400, detail="Сотрудник не может заменять сам себя.")
    if request.start_date > request.end_date:
         raise HTTPException(status_code=400, detail="Начальная дата не может быть позже конечной.")
    try:
        await database.setup_temporary_replacement(
            original_employee_id=request.original_employee_id,
            substitute_employee_id=request.substitute_employee_id,
            start_date=request.start_date,
            end_date=request.end_date
        )
        return {"status": "success", "message": "Временная замена успешно настроена."}
    except ValueError as ve:
         logger.warning(f"Ошибка при настройке замены: {ve}")
         raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"Ошибка при создании замены через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {e}")

@app.get("/api/replacements")
async def get_replacements(user: Annotated[dict, Depends(get_validated_user)]):
    """Возвращает список активных и будущих замен."""
    return await database.get_active_and_future_replacements()

@app.post("/api/replacements/cancel")
async def cancel_replacement_api(request: CancelReplacementRequest, user: Annotated[dict, Depends(get_validated_user)]):
    """Отменяет замену по ее ID."""
    try:
        await database.cancel_replacement(request.leave_id)
        return {"status": "success", "message": "Замена успешно отменена."}
    except Exception as e:
        logger.error(f"Ошибка отмены замены через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/monthly/{year}/{month}")
async def get_monthly_report(year: int, month: int, user: Annotated[dict, Depends(get_validated_user)]):
    try:
        report_data = await database.get_monthly_summary_data(year, month)
        if not report_data or len(report_data) <= 1:
            return {"data": []}
        return {"data": report_data}
    except Exception as e:
        logger.error(f"Ошибка при формировании месячного отчета через API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.post("/api/validate_user")
async def validate_user_initial(request: AuthRequest):
    try:
        user_info = await get_validated_user(request.initData)
        logger.info(f"Пользователь {user_info.get('id')} успешно прошел ПЕРВИЧНУЮ авторизацию в веб-панели.")
        return {"status": "ok", "user_id": user_info.get('id')}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Неожиданная ошибка при первичной валидации: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail="Некорректные данные для авторизации.")

@app.get("/")
async def read_root():
    """Отдает главную HTML страницу нашего веб-интерфейса."""
    return FileResponse('index.html')