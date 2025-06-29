import socket

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

if __name__ == "__main__":
    port = find_free_port()
    print(f"✅ Свободный порт найден: {port}")
    print(f"👉 Теперь используйте его для запуска uvicorn: uvicorn webapp:app --host 0.0.0.0 --port {port}")
