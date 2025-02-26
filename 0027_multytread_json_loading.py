"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0027_multytread_json_loading.py
"""

import json
import psycopg2
import time
import os
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser

# Настройки БД

def load_config( filename='database.ini', section='postgresql'):
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

# SQL-запросы
insert_chain_query = """
INSERT INTO chains (id, name, vector, dataset_id, file_id )
VALUES (%s, %s, %s, '0217ebce-ea19-11ef-b8ac-0242ac140004', '34981a88-ea19-11ef-bd95-0242ac140004')
"""

insert_markup_query = """
INSERT INTO markups (id, dataset_id, file_id, mark_frame, mark_time, vector, mark_path)
VALUES (%s, '0217ebce-ea19-11ef-b8ac-0242ac140004', '34981a88-ea19-11ef-bd95-0242ac140004', %s, %s, %s, %s)
"""

insert_chain_markup_query = """
INSERT INTO markups_chains (chain_id, markup_id)
VALUES (%s, %s)
"""

# Функция обработки одного JSON-файла
def process_json_file(file_name):
    cnt_markups = cnt_chains = 0
    start_time = time.time()
    print(f"Начало работы {file_name}: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

    file_path = f'json/0027/{file_name}'
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Подключение к PostgreSQL
    config = load_config()

    conn = psycopg2.connect(
        dbname=config['database'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
    )
    cursor = conn.cursor()

    # Обрабатываем файлы
    errors = success = 0  
    for file_entry in data.get("files", []):
        for chain in file_entry.get("file_chains", []):
            cnt_chains += 1 
            chain_id = chain["chain_id"]
            chain_name = chain["chain_name"]
            chain_vector = json.dumps(chain["chain_vector"])

            # Вставляем в таблицу chains
            # print("Executing SQL:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
            try:
                cursor.execute("SAVEPOINT sp1;")
                cursor.execute(insert_chain_query, (chain_id, chain_name, chain_vector))
                success += 1  # Счетчик успешных вставок
            except psycopg2.IntegrityError as e:
                cursor.execute("ROLLBACK TO SAVEPOINT sp1;")   # Очищаем ошибочное состояние транзакции
                errors += 1
                print(f"Ошибка целостности данных для markup_id={chain_id}: {e}")
            except psycopg2.DatabaseError as e:
                cursor.execute("ROLLBACK TO SAVEPOINT sp1;") 
                errors += 1
                print(f"Ошибка базы данных для markup_id={chain_id}: {e}")


            # for markup in chain.get("chain_markups", []):
            #     cnt_markups += 1
            #     markup_id = markup["markup_id"]
            #     markup_frame = markup["markup_frame"]
            #     markup_time = markup["markup_time"]
            #     markup_vector = json.dumps(markup["markup_vector"])
            #     markup_path = json.dumps(markup["markup_path"])

            #     # Вставляем в таблицу markups
            #     # print("Executing SQL:", cursor.mogrify(insert_markup_query, (markup_id, markup_frame, markup_time, json.dumps(markup_vector), markup_path)).decode("utf-8"))
           
            #     cursor.execute(insert_markup_query, (markup_id, markup_frame, markup_time, markup_vector, markup_path))

            #     # Вставляем связь chain_id ↔ markup_id в markups_chains
            #     cursor.execute(insert_chain_markup_query, (chain_id, markup_id))

    # Фиксация транзакции
    print(f'success:{success}, errors:{errors}')
    if success > 0:
        try:
            conn.commit()
            print(f"Успешно вставлено: {success} записей")
        except psycopg2.DatabaseError as e:
            conn.rollback()
            print(f"Ошибка при commit(): {e}")
    else:
        print("Все операции завершились ошибками, ничего не вставлено.")                
    # conn.commit()
    cursor.close()
    conn.close()
    end_time = time.time()
    print(f"Завершение работы {file_name}: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    print(f"Время затраченное на {file_name}: {end_time - start_time:.2f} сек")
    print(f'{file_name} done, chains:{cnt_chains}, markups:{cnt_markups}\n')

# Список JSON-файлов для обработки
json_files = [
    # "import1.json",
    # "import2.json",
    # "import3.json",
    # "import4.json",
    # "import5.json",
    # "import6.json",
    # "import7.json",
    # "import8.json",
    # "import9.json",
    "test.json",
]

start_script_time = time.time()
print(f"Начало работы скрипта: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_script_time))}\n")

# Запуск обработки файлов в нескольких потоках
with ThreadPoolExecutor(max_workers=5) as executor:  # 5 потоков (можно увеличить)
    executor.map(process_json_file, json_files)

end_script_time = time.time()
print(f"Завершение работы скрипта: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_script_time))}")
print(f"Общее время работы скрипта: {end_script_time - start_script_time:.2f} сек")