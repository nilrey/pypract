"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0025_db_camino_connect_alchemy.py 
"""
import os
import sys
import json
import threading
import time
import traceback
from sqlalchemy import text
import uuid
from datetime import datetime
from decimal import Decimal
from sqlalchemy import create_engine, text
from configparser import ConfigParser
from collections import defaultdict

class DatasetMarkupsExport:

    def __init__(self, project_id, dataset_id): 
        self.data_files = {}
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.status = {}
        self.errors = {}
        self.stop_event = threading.Event()
        self.threads = []
        self.monitor_thread = None
        self.wait_thread = None
        self.output_dir = 'json/0025' # f"/projects_data/{project_id}/{dataset_id}/markups_in" # директория с файлами json от ИНС
        self.logname = 'log.log'
        self.engine = create_engine(self.get_connection_string())

    def log_info(self, mes):
        os.makedirs('json/logs', exist_ok=True)
        with open( + f"json/logs/{self.logname}", "a") as file:
            file.write(f'{mes}\n')
 
    def load_config(self, filename='database.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)
        # get section, default to postgresql
        config = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                config[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return config
    
    def get_connection_string(self):
        config = self.load_config()
        cs = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        return cs

    def exec_query(self, query, params, convert = True):
        with self.engine.connect() as connection:
            result = connection.execute(query, params).mappings().all()
            if(convert):
                res = self.convert_exec_response(result)
            else:
                res = result
        return res
    
    def convert_exec_response(self, exec_response):
        result = [self.convert_to_serializable(dict(row)) for row in exec_response]
        return result
    
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
            return [DatasetMarkupsExport.convert_to_serializable(i) for i in obj]
        elif isinstance(obj, dict):  
            return {k: DatasetMarkupsExport.convert_to_serializable(v) for k, v in obj.items()}
        return obj  # Остальное оставляем без изменений
    
    def prepare_file(self, file_data):
        file = self.convert_to_serializable(dict(file_data)) 
        chains = self.prepare_chains(self.dataset_id , file['id']) # [{"id":1}, {"id":2}]
        return {'file_name' : file['name'],
                'file_id' : file['id'],
                'file_subset': 'teach',
                'file_chains' : self.convert_to_serializable(chains),
                }
    
    def prepare_chains(self, dataset_id, file_id):
        
        chains = self.get_chains(dataset_id, file_id)
        # self.log_info(f'Chains: {len(chains)}' )
        chains_formatted = {}
        #chains_formatted = self.convert_to_serializable(dict(chains)) 
        # chains_markups = self.get_chains_markups(dataset_id, file_id)
        
        # grouped_data = defaultdict(list)
        # cname_map = {}
        # # Заполняем словарь
        total = len(chains)
        for idx, chain in enumerate(chains, start=1): 
            markups = self.get_markups(chain['cid']) 
            print(f"{idx} из {total} ({int(idx/(total/100))}%)")
            chain["chain_markups"] = markups
            chain.pop("cid", None)
        #     cid, chain_name, chain_vector = data.values()
        #     cname_map[cid] = chain_name  # Запоминаем имя, чтобы не записывать в каждой итерации
        #     #grouped_data[cid].append({"markup_frame": markup_frame,"markup_time": markup_time,"markup_vector": markup_vector, "markup_path": markup_path })

        # # Преобразуем в нужный формат
        # chains_formatted = [{"chain_name": cname_map[cid], "chain_vector": chain_vector, "chain_markups": markups} for cid, markups in grouped_data.items()]

        return chains 
    
    def get_json_data(self, file_data):
        datasets = self.get_binded_datasets()
        file = self.prepare_file(file_data)
        return {'datasets': datasets, 'files': {'file': file} }
    

    def create_json_file(self, file_data):
        # path_to_file = self.output_dir #'json/0018/'
        #self.log_info(f"путь к файлу: {path_to_file}/{file_data['name']}")
        json_data = self.get_json_data(file_data)

        try:
            os.makedirs(self.output_dir, exist_ok=True)  # Создаем каталог, если его нет
            file_path = os.path.join(self.output_dir, f"{file_data['name']}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                # if (self.get_dataset_parent_id(datasets) is not None ):
                json.dump(json_data, f, ensure_ascii=False, indent=4)
                self.message = 'Success'
                # else:
                #     self.message = 'Error: dataset_parent_id is not found '

            self.status[file_data['name']] = "Success"
            #self.log_info(self.status[file_data['name']])
        except Exception as e:
            self.status[file_data['name']] = "Failed"
            self.errors[file_data['name']] = traceback.format_exc()
            #self.log_info(self.status[file_data['name']])
            #self.log_info(self.errors[file_data['name']])

    def monitor_threads(self):
        # Менеджер потоков - для отслеживания состояния других потоков
        while not self.stop_event.is_set():
            all_finished = True
            for filename, state in list(self.status.items()):
                if state not in ["Success", "Failed"]:
                    all_finished = False
                elif state is not None:
                    #print(f"{state} - {filename}")
                    self.status[filename] = None  # Чтобы не дублировать вывод
            
            if all_finished:
                self.stop_event.set()
                break  

        #print("\nSummary:")
        for filename, state in self.status.items():
            if state is None:
                state = "Success"
           # print(f"{filename}: {state}")

        if self.errors:
            print("\nErrors:")
            for filename, error in self.errors.items():
                print(f"{filename} failed with error:\n{error}")

    def wait_for_threads(self):
        # Ожидание завершения всех потоков. 
        for thread in self.threads:
            thread.join()
        self.stop_event.set()
        self.monitor_thread.join()
        print("Monitoring thread has finished.") # START DOCKER CREATE CONTAINER

    def run(self):
        ##self.log_info("Start process")
        self.data_files = self.get_dataset_files(self.dataset_id)
        #self.log_info(len(self.data_files))
        # Запуск потоков создания файлов
        self.status = {filename: "In Progress" for filename in self.data_files}
        # print(f"{resp[0]['id']}", file=sys.stderr)
        for file in self.data_files:
            thread = threading.Thread(target=self.create_json_file, args=(file,))
            thread.start()
            self.threads.append(thread)

        # Запуск мониторинга
        self.monitor_thread = threading.Thread(target=self.monitor_threads)
        self.monitor_thread.start()

        # Запуск ожидания завершения в отдельном потоке
        self.wait_thread = threading.Thread(target=self.wait_for_threads)
        self.wait_thread.start()
        #files_count = len(self.data_files)
        message = {'message': 'Файлы отправлены в обработку', 'files_count' : 1 }
        #self.log_info( message )
        
        return message

    def get_dataset_files(self, dataset_id):
        # получим корневой dataset id
        stmt = text("SELECT d2.id FROM datasets d1 , datasets d2 where d1.project_id = d2.project_id and d2.parent_id is null and d1.id = :dataset_id")
        parent_dataset_id = self.exec_query(stmt, {"dataset_id" : dataset_id } )
        #self.log_info(f'dataset_id = {dataset_id}')
        #self.log_info(f'parent_dataset_id = {parent_dataset_id[0]["id"]}')

        stmt = text("SELECT * FROM files f  WHERE f.dataset_id = :dataset_id")
        files = self.exec_query(stmt, {"dataset_id" : parent_dataset_id[0]['id']} , False)
        #print(f"{resp}", file=sys.stderr)
        return files
    
    def stmt_binded_datasets(self):
        return text("""
            WITH RECURSIVE dataset_hierarchy AS (
                SELECT id, parent_id, name, type_id
                FROM public.datasets
                WHERE id = :dataset_id
                UNION ALL
                SELECT d.id, d.parent_id, d.name, d.type_id
                FROM public.datasets d
                JOIN dataset_hierarchy dh ON d.id = dh.parent_id
            )
            SELECT * FROM dataset_hierarchy;
        """)

    def get_binded_datasets(self):
        datasets = self.exec_query( self.stmt_binded_datasets(), {"dataset_id": self.dataset_id })

        key_mapping = {
            "id": "dataset_id",
            "parent_id": "dataset_parent_id",
            "name": "dataset_name",
            "type_id": "dataset_type"
        }
        type_mapping = {0: "initial", 1: "frame"}

        renamed_datasets = [
            {key_mapping[k]: (type_mapping[v] if k == "type_id" else v) for k, v in d.items()}
            for d in datasets
        ]
        return renamed_datasets
    
    def get_dataset_parent_id(self, rows):
        for row in rows:
            if row['parent_id'] == None:
                self.dataset_parent_id = row['id']
        return self.dataset_parent_id
    
    def stmt_chains(self):
        return text("""
            select c.id as cid, c.name as chain_name, c.vector as chain_vector from chains c
            where c.dataset_id = :dataset_id
            AND c.file_id = :file_id
            order by cid
        """)
        
    def get_chains(self, dataset_id, file_id):
        chains = self.exec_query( self.stmt_chains(), {"dataset_id": dataset_id, "file_id": file_id })
        return chains
    
    def stmt_markups(self):
        return text("""
            select m.mark_frame as markup_frame, m.mark_time as markup_time, m.vector as markup_vector, m.mark_path as markup_path 
                from markups_chains mc
            join markups m on mc.markup_id = m.id 
            where mc.chain_id = :chain_id
            AND m.is_deleted = false
            AND NOT EXISTS (
                SELECT 1 FROM markups m2 WHERE m2.previous_id = m.id 
            )
        """)
        
    def get_markups(self, chain_id):
        markups = self.exec_query( self.stmt_markups(), {"chain_id": chain_id })
        print(f'markups: {len(markups)}')
        return markups

    
    
    def stmt_binded_chains_markups(self):
        return text("""
            select c.id as cid, c.name as chain_name, c.vector as chain_vector, m.id as mid, m.mark_frame as markup_frame, m.mark_time as markup_time, m.vector as markup_vector, m.mark_path as markup_path from chains c
            join markups_chains mc on c.id = mc.chain_id
            join markups m on mc.markup_id = m.id 
            where c.dataset_id = :dataset_id
            AND c.file_id = :file_id
            AND m.is_deleted = false
            AND NOT EXISTS (
                SELECT 1 FROM markups m2 WHERE m2.previous_id = m.id
            )
            order by cid
        """)

    def get_chains_markups(self, dataset_id, file_id):
        # print(dataset_id)
        chains_markups = self.exec_query( self.stmt_binded_chains_markups(), {"dataset_id": dataset_id, "file_id": file_id })
        return chains_markups

if __name__ == "__main__":
    # project_id='fc3108a6-7b57-11ef-b77b-0242ac140002'
    # dataset_id='03dfeb68-7cb5-11ef-84e7-0242ac140002'
    project_id='32c92072-857d-11ef-8c09-0242ac140002'
    dataset_id='b1e57392-87c1-11ef-8658-0242ac140002'
    manager = DatasetMarkupsExport(project_id, dataset_id)
    manager.run()
