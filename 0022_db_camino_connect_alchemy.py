"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0021_db_camino_connect.py 
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

def convert_to_serializable(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)  # Преобразуем UUID в строку
    elif isinstance(obj, datetime):
        return obj.isoformat()  # Дата -> строка ISO 8601
    elif isinstance(obj, Decimal):
        return float(obj)  # Decimal -> float
    elif isinstance(obj, list):  # Обрабатываем списки рекурсивно
        return [convert_to_serializable(i) for i in obj]
    elif isinstance(obj, dict):  # Обрабатываем словари рекурсивно
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    return obj  # Оставляем без изменений, если не требует обработки

# Создание строки подключения для SQLAlchemy
DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database}"

# Подключение к базе данных (замени параметры на свои)
engine = create_engine(DATABASE_URL)

# Выполняем запрос
with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM markups LIMIT 10"))
    rows = [dict(row) for row in result.mappings().all()]  # Преобразуем в список словарей

# Определяем путь сохранения
output_dir = "json/markups"
os.makedirs(output_dir, exist_ok=True)  # Создаем каталог, если его нет
output_file = os.path.join(output_dir, "markups.json")

# Сохраняем в JSON, учитывая возможные UUID
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, indent=4, default=convert_to_serializable)


print(f"Данные сохранены в {output_file}")
