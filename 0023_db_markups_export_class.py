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
    def __init__(self):
        self.project_id = self.dataset_id = self.dataset_parent_id = self.output_dir = None
        self.engine = create_engine(self.get_db_url())
        self.message = ''
        # self.output_dir = f'/projects_data/{project_id}/{dataset_id}/markups_in'
        

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
    
    def stmt_chains_markups(self):
        stmt = text("""
            select c.id as cid, c.name, c.dataset_id, c.file_id, c.is_deleted, c.is_verified, m.id as mid, m.parent_id, m.mark_time from chains c
            join markups_chains mc on c.id = mc.chain_id
            join markups m on mc.markup_id = m.id 
            where c.dataset_id = :dataset_id
            AND c.file_id = :file_id
            AND m.is_deleted = false
            AND NOT EXISTS (
                SELECT 1 FROM markups m2 WHERE m2.previous_id = m.id
            )
        """)
        return stmt
    
    def stmt_binded_datasets(self):
        stmt = text("""
            WITH RECURSIVE dataset_hierarchy AS (
                SELECT id, parent_id, name, type_id, project_id, source, nn_original_id, nn_online_id,
                    nn_teached_id, description, author_id, dt_created, dt_calculated, is_calculated, is_deleted
                FROM public.datasets
                WHERE id = :dataset_id
                UNION ALL
                SELECT d.id, d.parent_id, d.name, d.type_id, d.project_id, d.source,d.nn_original_id,d.nn_online_id,
                    d.nn_teached_id,d.description,d.author_id,d.dt_created,d.dt_calculated,d.is_calculated,d.is_deleted
                FROM public.datasets d
                JOIN dataset_hierarchy dh ON d.id = dh.parent_id
            )
            SELECT * FROM dataset_hierarchy;
        """)
        return stmt

    # def export_markups(self, limit=10, output_file="markups.json"):
    #     query = self.stmt_chains_markups()
        
    #     with self.engine.connect() as connection:
    #         result = connection.execute(query, {"dataset_id": self.dataset_id, "file_id": self.file_id }) 
    #         rows = [self.convert_to_serializable(dict(row)) for row in result.mappings().all()]

    #     file_path = os.path.join(self.output_dir, output_file)
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(rows, f, ensure_ascii=False, indent=4)

    #     print(f"Данные успешно сохранены в {file_path}")

    def exec_query(self, query, params):
        with self.engine.connect() as connection:
            result = connection.execute(query, params) 
            res = [self.convert_to_serializable(dict(row)) for row in result.mappings().all()]

        return res

    def get_binded_datasets(self):
        res = self.exec_query( self.stmt_binded_datasets(), {"dataset_id": self.dataset_id })
        return res
    
    def get_dataset_prent_id(self, rows):
        for row in rows:
            if row['parent_id'] == None:
                self.dataset_parent_id = row['id']
        return self.dataset_parent_id

    def run(self, image_id, params , output_file="markups.json"):
        self.project_id, self.dataset_id = params['markups'].strip("/").split("/")[-3:-1]
        self.output_dir = f'json/{self.project_id}/{self.dataset_id}/markups_in'
        # get datasets_ids link 
        datasets = self.get_binded_datasets()
        # files_ids
        
        os.makedirs(self.output_dir, exist_ok=True)  # Создаем каталог, если его нет
        file_path = os.path.join(self.output_dir, output_file)
        with open(file_path, "w", encoding="utf-8") as f:
            if (self.get_dataset_prent_id(datasets) is not None ):
                json.dump({'datasets':datasets}, f, ensure_ascii=False, indent=4)
                self.message = 'Success'
            else:
                self.message = 'Error: dataset_parent_id is not found '

        # self.export_markups()
        return self.message


if __name__ == "__main__":
    image_id = 1
    post_params = {'markups': "/projects_data/fc3108a6-7b57-11ef-b77b-0242ac140002/03dfeb68-7cb5-11ef-84e7-0242ac140002/markups_out"}
    exporter = MarkupExporter()  # Создаем объект экспортера
    res = exporter.run(image_id, post_params)  # Экспортируем данные
    print(res)
