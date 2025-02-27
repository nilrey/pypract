"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0030_multytread_json_loading.py
"""

import json
import psycopg2
import time
import os
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
import logging

# Настройки БД

LOG_PATH = 'json/0030'
LOG_FNAME = 'log.log'
LOG_FILE = f'{LOG_PATH}/{LOG_FNAME}'

def init_logger(type = 'file'):
    os.makedirs(LOG_PATH, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG) 
    if(type == 'console'):    
        # вывод в консоль
        handler = logging.StreamHandler()
    else:
        # вывод в файл
        handler = logging.FileHandler(f"{LOG_FILE}", encoding="utf-8")
    
    handler.setLevel(logging.DEBUG)
    # Определяем формат сообщений
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Добавляем обработчик к логгеру (если он ещё не добавлен)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

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

def exec_query(cursor, method_name, query_params):
    markup_success = 0
    try:
        cursor.execute("SAVEPOINT spMarkup;")
        cursor.execute(method_name, query_params)
        markup_success += 1
    except psycopg2.IntegrityError as e:
        cursor.execute("ROLLBACK TO SAVEPOINT spMarkup;")   # Очищаем ошибочное состояние транзакции
        print(f"Ошибка целостности данных: {e}")
        # print("Запрос вызвавший ошибку:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
    except psycopg2.DatabaseError as e:
        cursor.execute("ROLLBACK TO SAVEPOINT spMarkup;") 
        print(f"Ошибка базы данных: {e}")
        # print("Запрос с ошибкой:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
    finally:
        cursor.execute("RELEASE SAVEPOINT spMarkup;") 

    return markup_success

# Функция обработки одного JSON-файла
def process_json_file(file_name):
    cnt_markups = cnt_chains = 0
    start_time = time.time()
    mes = f"Начало обработки {file_name}"
    log.info(mes)
    print(mes)

    file_path = f'json/0027/{file_name}'
    # log.info(file_path)
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
    chain_errors = chain_success = markup_errors = markup_success = 0  
    for file_entry in data.get("files", []):
        for chain in file_entry.get("file_chains", []):
            cnt_chains += 1 
            chain_id = chain["chain_id"]
            chain_name = chain["chain_name"]
            chain_vector = json.dumps(chain["chain_vector"])

            # Вставляем в таблицу chains
            try:
                cursor.execute("SAVEPOINT spChain;")
                cursor.execute(insert_chain_query, (chain_id, chain_name, chain_vector))
                chain_success += 1  

                for markup in chain.get("chain_markups", []):
                    cnt_markups += 1
                    markup_id = markup["markup_id"]
                    markup_frame = markup["markup_frame"]
                    markup_time = markup["markup_time"]
                    markup_vector = json.dumps(markup["markup_vector"])
                    markup_path = json.dumps(markup["markup_path"])

                    # Вставляем в таблицу markups
                    # print("Executing SQL:", cursor.mogrify(insert_markup_query, (markup_id, markup_frame, markup_time, json.dumps(markup_vector), markup_path)).decode("utf-8"))
                    markup_success += exec_query(cursor, insert_markup_query, (markup_id, markup_frame, markup_time, markup_vector, markup_path))
                    # print(f'\nResult of exec_query = {res}\n')
                    # try:
                    #     cursor.execute("SAVEPOINT spMarkup;")
                    #     cursor.execute(insert_markup_query, (markup_id, markup_frame, markup_time, markup_vector, markup_path))
                    #     markup_success += 1
                    # except psycopg2.IntegrityError as e:
                    #     cursor.execute("ROLLBACK TO SAVEPOINT spMarkup;")   # Очищаем ошибочное состояние транзакции
                    #     markup_errors += 1
                    #     print(f"Ошибка целостности данных для markup_id={markup_id}: {e}")
                    #     # print("Запрос вызвавший ошибку:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
                    # except psycopg2.DatabaseError as e:
                    #     cursor.execute("ROLLBACK TO SAVEPOINT spMarkup;") 
                    #     markup_errors += 1
                    #     print(f"Ошибка базы данных для markup_id={markup_id}: {e}")
                    #     # print("Запрос с ошибкой:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
                    # finally:
                    #     cursor.execute("RELEASE SAVEPOINT spMarkup;") 

                    # Вставляем связь chain_id ↔ markup_id в markups_chains
                    cursor.execute(insert_chain_markup_query, (chain_id, markup_id))

            except psycopg2.IntegrityError as e:
                cursor.execute("ROLLBACK TO SAVEPOINT spChain;")   # Очищаем ошибочное состояние транзакции
                chain_errors += 1
                # print(f"Ошибка целостности данных для chain_id={chain_id}: {e}")
                # print("Запрос с ошибкой:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
            except psycopg2.DatabaseError as e:
                cursor.execute("ROLLBACK TO SAVEPOINT spChain;") 
                chain_errors += 1
                # print(f"Ошибка базы данных для chain_id={chain_id}: {e}")
                # print("Запрос с ошибкой:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )

            finally:
                cursor.execute("RELEASE SAVEPOINT spChain;") 

    # Фиксация транзакции
    mes = f'\nchain_success:{chain_success}, chain_errors:{chain_errors}, markup_success:{markup_success}, markup_errors:{markup_errors}'
    print(mes)
    log.info(mes)
    if chain_success > 0 and markup_success > 0 :
        try:
            conn.commit()
            # print(f"Успешно добавлено: {chain_success+markup_success} записей")
        except psycopg2.DatabaseError as e:
            conn.rollback()
            print(f"Ошибка при commit(): {e}")
    else:
        print("Все операции завершились ошибками, ничего не добавлено.")                
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

log = init_logger() 
start_script_time = time.time()
print(f"Начало работы скрипта: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_script_time))}\n")

# Запуск обработки файлов в нескольких потоках
with ThreadPoolExecutor(max_workers=2) as executor:  # 5 потоков (можно увеличить)
    executor.map(process_json_file, json_files)

end_script_time = time.time()
print(f"Завершение работы скрипта: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_script_time))}")
print(f"Общее время работы скрипта: {end_script_time - start_script_time:.2f} сек")