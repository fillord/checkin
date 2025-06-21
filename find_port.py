# find_port.py
import socket

def find_free_port():
    """
    Находит свободный TCP порт, доступный в системе.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Привязываем сокет к порту 0. ОС сама выделит свободный номер порта.
        s.bind(('', 0))
        # Узнаем, какой именно порт был выделен, и возвращаем его.
        return s.getsockname()[1]

if __name__ == "__main__":
    port = find_free_port()
    print(f"✅ Свободный порт найден: {port}")
    print(f"👉 Теперь используйте его для запуска uvicorn: uvicorn webapp:app --host 0.0.0.0 --port {port}")
