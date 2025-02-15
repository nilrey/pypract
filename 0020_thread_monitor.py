import os
import json
import threading
import time
import traceback

class JsonFileManager:
    def __init__(self, data_files):
        """
        Инициализация менеджера файлов.
        :param data_files: словарь {filename: (data, delay)}
        """
        self.data_files = data_files
        self.status = {filename: "In Progress" for filename in data_files}
        self.errors = {}
        self.stop_event = threading.Event()
        self.threads = []
        self.monitor_thread = None
        self.wait_thread = None

    def create_json_file(self, filename, data, delay):
        """Создание JSON-файла с задержкой и обработкой ошибок."""
        try:
            path_to_file = 'json/0018/'
            os.makedirs(path_to_file, exist_ok=True) 
            time.sleep(delay)  # Имитация задержки
            with open(path_to_file+filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            self.status[filename] = "Success"
        except Exception as e:
            self.status[filename] = "Failed"
            self.errors[filename] = traceback.format_exc()

    def monitor_threads(self):
        """Фоновый поток для отслеживания состояния других потоков."""
        while not self.stop_event.is_set():
            all_finished = True
            for filename, state in list(self.status.items()):
                if state not in ["Success", "Failed"]:
                    all_finished = False
                elif state is not None:
                    print(f"{state} - {filename}")
                    self.status[filename] = None  # Чтобы не дублировать вывод
            
            if all_finished:
                time.sleep(0.5)  # Дать время последнему обновлению
                self.stop_event.set()
                break  

            time.sleep(1)

        print("\nSummary:")
        for filename, state in self.status.items():
            if state is None:
                state = "Success"
            print(f"{filename}: {state}")

        if self.errors:
            print("\nErrors:")
            for filename, error in self.errors.items():
                print(f"{filename} failed with error:\n{error}")

    def wait_for_threads(self):
        """Ожидание завершения всех потоков."""
        for thread in self.threads:
            thread.join()
        self.stop_event.set()
        self.monitor_thread.join()
        print("Monitoring thread has finished.") # START DOCKER CREATE CONTAINER

    def run(self):
        """Запуск всех потоков."""
        # Запуск потоков создания файлов
        for filename, (data, delay) in self.data_files.items():
            thread = threading.Thread(target=self.create_json_file, args=(filename, data, delay))
            thread.start()
            self.threads.append(thread)

        # Запуск мониторинга
        self.monitor_thread = threading.Thread(target=self.monitor_threads)
        self.monitor_thread.start()

        print("Main thread is free and doing other work...\n")

        # Запуск ожидания завершения в отдельном потоке
        self.wait_thread = threading.Thread(target=self.wait_for_threads)
        self.wait_thread.start()
        
        print("send HTTP response")


if __name__ == "__main__":
    data_files = {
        "file1.json": ({"name": "Alice", "age": 25}, 2),
        "file2.json": ({"name": "Bob", "age": 30}, 4),
        "file3.json": ({"name": "Charlie", "age": 35}, 6),
    }

    manager = JsonFileManager(data_files)
    manager.run()
