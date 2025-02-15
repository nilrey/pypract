import json
import threading
import time
import traceback

def create_json_file(filename, data, delay, status_dict, error_dict):
    """Функция потока для создания JSON-файла с обработкой ошибок."""
    try:
        time.sleep(delay)  # Имитация задержки
        # if filename == "file2.json": raise ValueError("Ошибка при записи файла!")  # Для теста ошибки
        
        with open('json/0018/'+filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        status_dict[filename] = "Success"
    except Exception as e:
        status_dict[filename] = "Failed"
        error_dict[filename] = traceback.format_exc()  # Сохраняем стек ошибки

def monitor_threads(status_dict, error_dict, stop_event):
    """Фоновый поток для отслеживания состояния других потоков."""
    while not stop_event.is_set():
        all_finished = True  # Флаг завершения
        for filename, state in list(status_dict.items()):
            if state not in ["Success", "Failed"]:  
                all_finished = False  # Если есть незавершённые потоки, продолжаем работу
            elif state is not None:  # Выводим статус только один раз
                print(f"{state} - {filename}")
                status_dict[filename] = None  # Чтобы не дублировать вывод
        
        if all_finished:  
            stop_event.set()  # Даем сигнал на остановку мониторинга
            break  # Выходим из цикла
        
        time.sleep(1)  # Проверяем статус раз в секунду

    print("\nSummary:")
    for filename, state in status_dict.items():
        if state is None:
            state = "Success"  # Подстраховка на случай, если статус уже сброшен
        print(f"{filename}: {state}")

    if error_dict:
        print("\nErrors:")
        for filename, error in error_dict.items():
            print(f"{filename} failed with error:\n{error}")

# Данные для записи
data_files = {
    "file1.json": ({"name": "Alice", "age": 25}, 2),
    "file2.json": ({"name": "Bob", "age": 30}, 4),  # Можно добавить ошибку для теста
    "file3.json": ({"name": "Charlie", "age": 35}, 6),
}

# Словари для статуса и ошибок
status = {filename: "In Progress" for filename in data_files}
errors = {}

# Создаём объект события для остановки потока
stop_event = threading.Event()

# Запускаем потоки для создания файлов
threads = []
for filename, (data, delay) in data_files.items():
    thread = threading.Thread(target=create_json_file, args=(filename, data, delay, status, errors))
    thread.start()
    threads.append(thread)

# Запускаем монитор в отдельном потоке
monitor_thread = threading.Thread(target=monitor_threads, args=(status, errors, stop_event))
monitor_thread.start()

print("Main thread is free and doing other work...\n")

# Ждём завершения рабочих потоков
for thread in threads:
    thread.join()

# Ожидаем завершения мониторинга
stop_event.set()  # Говорим монитору завершиться
monitor_thread.join()

print("Monitoring thread has finished.")
