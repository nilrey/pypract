"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0022_db_camino_connect_alchemy.py 
"""
from sqlalchemy import create_engine, text
import json
import os
import uuid
from datetime import datetime
from decimal import Decimal

username = 'postgres'  # имя пользователя
password = 'postgres'  # пароль
host = 'localhost'  # хост (или IP контейнера, если нужно)
port = '5432'   
database = 'camino'   

# Создание строки подключения для SQLAlchemy
DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database}"

# Подключение к базе данных (замени параметры на свои)
engine = create_engine(DATABASE_URL)


# Функция для приведения данных к JSON-совместимому виду
def convert_to_serializable(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)  # Преобразуем UUID в строку
    elif isinstance(obj, datetime):
        return obj.isoformat()  # Дата -> строка ISO 8601
    elif isinstance(obj, Decimal):
        return float(obj)  # Decimal -> float
    elif isinstance(obj, str):  # Попытка распарсить JSON-строку
        try:
            parsed_json = json.loads(obj)  # Если строка содержит JSON, преобразуем её
            return parsed_json
        except (json.JSONDecodeError, TypeError):
            return obj  # Если не JSON, оставляем как есть
    elif isinstance(obj, list):  # Обрабатываем списки рекурсивно
        return [convert_to_serializable(i) for i in obj]
    elif isinstance(obj, dict):  # Обрабатываем словари рекурсивно
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    return obj  # Оставляем без изменений, если не требует обработки

# Выполняем запрос
with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM markups LIMIT 10"))
    rows = [convert_to_serializable(dict(row)) for row in result.mappings().all()]  # Преобразуем в список словарей

# Определяем путь сохранения
output_dir = "json/markups"
os.makedirs(output_dir, exist_ok=True)  # Создаем каталог, если его нет
output_file = os.path.join(output_dir, "markups.json")

# Сохраняем в JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, indent=4)
    
print(f"Данные сохранены в {output_file}")
