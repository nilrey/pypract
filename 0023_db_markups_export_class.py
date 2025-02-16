"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0022_db_camino_connect_alchemy.py 
"""
import json
import os
import uuid
from datetime import datetime
from decimal import Decimal
from sqlalchemy import create_engine, text

class MarkupExporter:
    def __init__(self, markups_path):
        project_id, dateset_id = markups_path.strip("/").split("/")[-3:-1]

        self.engine = create_engine(self.get_db_url())
        # self.output_dir = f'/projects_data/{project_id}/{dateset_id}/markups_in'
        self.output_dir = f'json/{project_id}/{dateset_id}/markups_in'
        os.makedirs(self.output_dir, exist_ok=True)  # Создаем каталог, если его нет

    def get_db_url(self):
        username = 'postgres'  # имя пользователя
        password = 'postgres'  # пароль
        host = 'localhost'  # хост (или IP контейнера, если нужно)
        port = '5432'   
        database = 'camino'   
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"

    @staticmethod
    def convert_to_serializable(obj):
        # Преобразует объекты, которые не поддерживаются JSON (UUID, datetime, Decimal, JSON-строки).
        if isinstance(obj, uuid.UUID):
            return str(obj)  # UUID -> строка
        elif isinstance(obj, datetime):
            return obj.isoformat()  # Дата -> ISO 8601
        elif isinstance(obj, Decimal):
            return float(obj)  # Decimal -> float
        elif isinstance(obj, str):  
            try:
                return json.loads(obj)  # Пробуем распарсить JSON-строку
            except (json.JSONDecodeError, TypeError):
                return obj  # Оставляем как есть, если это не JSON
        elif isinstance(obj, list):  
            return [MarkupExporter.convert_to_serializable(i) for i in obj]
        elif isinstance(obj, dict):  
            return {k: MarkupExporter.convert_to_serializable(v) for k, v in obj.items()}
        return obj  # Остальное оставляем без изменений

    def export_markups(self, limit=10, output_file="markups.json"):
        query = text(f"SELECT * FROM markups LIMIT {limit}")
        
        with self.engine.connect() as connection:
            result = connection.execute(query)
            rows = [self.convert_to_serializable(dict(row)) for row in result.mappings().all()]

        file_path = os.path.join(self.output_dir, output_file)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=4)

        print(f"Данные успешно сохранены в {file_path}")

if __name__ == "__main__":
    markups =  "/projects_data/fc3108a6-7b57-11ef-b77b-0242ac140002/03dfeb68-7cb5-11ef-84e7-0242ac140002/markups_out"
    exporter = MarkupExporter(markups)  # Создаем объект экспортера
    exporter.export_markups(limit=10)  # Экспортируем данные
