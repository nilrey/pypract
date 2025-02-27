import traceback
import logging
import os
class Example:
    def __init__(self, filename):
        self.logger = self.init_logger() 
            
        self.logger.info("__init__ called")
        self.file = open(filename, 'w')

    def init_logger(self):
        path_logfile = 'json/0031/'
        os.makedirs(path_logfile, exist_ok=True)
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG) 

        # Создаём обработчик вывода в консоль
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        # Обработчик для записи в файл
        file_handler = logging.FileHandler(f"{path_logfile}app.log", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        # Определяем формат сообщений
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Добавляем обработчик к логгеру (если он ещё не добавлен)
        if not logger.hasHandlers():
            logger.addHandler(file_handler)

        return logger

    def __enter__(self): 
        self.logger.info("Выполняется метод __enter__")
        return self.file  # Можно вернуть сам файл или self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.logger.info("__exit__ called")
        self.file.write(f"{exc_type.__name__}: {exc_value}\n")
        self.file.writelines(traceback.format_tb(exc_traceback) )
        self.file.close()
        return True 

print("Before with block")
with Example("0031_test.txt") as f:
    print("Inside with block")
    1/0
    f.write("Hello, world!")  # Запись в файл
print("After with block")
