# app_context.py
import logging
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger(__name__)

# Мы больше не создаем пул сразу, а просто объявляем переменную
_process_pool_executor = None

def get_process_pool_executor() -> ProcessPoolExecutor:
    """
    Возвращает существующий пул процессов или создает новый, если его еще нет.
    Это гарантирует, что тяжелая операция создания выполнится только один раз.
    """
    global _process_pool_executor
    if _process_pool_executor is None:
        logger.info("Первый запрос на распознавание лиц. Создание пула процессов...")
        _process_pool_executor = ProcessPoolExecutor()
        logger.info("Пул процессов успешно создан.")
    return _process_pool_executor

def shutdown_executor():
    """Корректно закрывает пул процессов, если он был создан."""
    global _process_pool_executor
    if _process_pool_executor:
        logger.info("Закрытие пула процессов...")
        _process_pool_executor.shutdown()