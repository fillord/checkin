try:
    print("Проверка импорта face_recognition...")
    import face_recognition
    print("✅ УСПЕХ! Библиотека 'face_recognition' и ее зависимости установлены правильно.")
    print("\nТеперь вы можете запускать основного бота командой:")
    print("python bot.py")
except Exception as e:
    print(f"\n❌ ОШИБКА: Проблема все еще существует.")
    print(f"   Подробности: {e}")