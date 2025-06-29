import logging
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger(__name__)

_process_pool_executor = None

def get_process_pool_executor() -> ProcessPoolExecutor:
    global _process_pool_executor
    if _process_pool_executor is None:
        logger.info("Первый запрос на распознавание лиц. Создание пула процессов...")
        _process_pool_executor = ProcessPoolExecutor()
        logger.info("Пул процессов успешно создан.")
    return _process_pool_executor

def shutdown_executor():
    global _process_pool_executor
    if _process_pool_executor:
        logger.info("Закрытие пула процессов...")
        _process_pool_executor.shutdown()