import threading
import time


class CustomThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._status = "initialized"  # Добавляем пользовательский статус

    def run(self):
        self._status = "running"
        super().run()
        self._status = "finished"

    def get_status(self):
        return self._status


def worker():
    time.sleep(3)
    print("Работа завершена")


thread = CustomThread(target=worker)
print(f"Состояние перед стартом: {thread.get_status()}")  # initialized
thread.start()
print(f"Состояние после старта: {thread.get_status()}")   # running
thread.join()
print(f"Состояние после завершения: {thread.get_status()}")  # finished
