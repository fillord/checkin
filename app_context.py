# app_context.py
from concurrent.futures import ProcessPoolExecutor

# Глобальный пул процессов для тяжелых вычислений (распознавание лиц),
# доступный для импорта в других модулях.
process_pool_executor = ProcessPoolExecutor()