import json
import threading
import time
import traceback

def create_json_file(filename, data, delay, status_dict, error_dict):
    try:
        time.sleep(delay)  # Имитация задержки
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        status_dict[filename] = True  # Отмечаем, что поток завершился
    except Exception as e:
        status_dict[filename] = "❌ Failed"
        error_dict[filename] = traceback.format_exc()  # Сохраняем стек ошибки

# Данные для записи
data_files = {
    "file1.json": ({"name": "Alice", "age": 25}, 2),
    "file2.json": ({"name": "Bob", "age": 30}, 4),
    "file3.json": ({"name": "Charlie", "age": 35}, 6),
}

# Словарь для отслеживания состояния потоков
status = {filename: False for filename in data_files}
errors = {}

# Создаем и запускаем потоки
threads = []
for filename, (data, delay) in data_files.items():
    thread = threading.Thread(target=create_json_file, args=(filename, data, delay, status, errors))
    thread.start()
    threads.append(thread)

# Основной поток продолжает работать
print("Main thread is free and doing other work...\n")

# Отслеживание состояния потоков без блокировки
while not all(status.values()):  # Пока не все потоки завершились
    for filename, done in status.items():
        if done:
            print(f"✅ {filename} has been created.")
            status[filename] = None  # Чтобы не выводить повторно
    time.sleep(1)  # Проверяем раз в секунду

print("\nAll background threads have finished!")
