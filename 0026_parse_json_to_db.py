"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0026_parse_json_to_db.py
"""
import json
import psycopg2
from configparser import ConfigParser

# Открываем JSON-файл
with open("json/0026/test2.json", "r", encoding="utf-8") as file:
    data = json.load(file)

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

# def get_connection_string(self):
#     config = self.load_config()
#     cs = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
#     return cs

config = load_config()

# Подключение к PostgreSQL
conn = psycopg2.connect(
    dbname=config['database'],
    user=config['user'],
    password=config['password'],
    host=config['host'],
    port=config['port'],
)
cursor = conn.cursor()

# SQL-запросы для вставки данных
insert_chain_query = """
INSERT INTO chains (id, name, vector, dataset_id, file_id )
VALUES (%s, %s, %s, '0217ebce-ea19-11ef-b8ac-0242ac140004', '34981a88-ea19-11ef-bd95-0242ac140004')
ON CONFLICT (id) DO NOTHING;
"""

insert_markup_query = """
INSERT INTO markups (id, dataset_id, file_id, mark_frame, mark_time, vector, mark_path)
VALUES (%s, '0217ebce-ea19-11ef-b8ac-0242ac140004', '34981a88-ea19-11ef-bd95-0242ac140004', %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

insert_chain_markup_query = """
INSERT INTO markups_chains (chain_id, markup_id)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
"""
cnt_markups = cnt_chains = 0

# Обработка данных
for file_entry in data.get("files", []):
    for chain in file_entry.get("file_chains", []):
        cnt_chains += 1
        chain_id = chain["chain_id"]
        chain_name = chain["chain_name"]
        chain_vector = chain["chain_vector"]
        
        # Вставка данных в таблицу chains            
        # print("Executing SQL:", cursor.mogrify ( insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)) ).decode("utf-8") )
        cursor.execute(insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)))

        # cursor.execute(insert_chain_query, (chain_id, chain_name, json.dumps(chain_vector)))
        
        for markup in chain.get("chain_markups", []):
            cnt_markups += 1
            markup_id = markup["markup_id"]
            markup_frame = markup["markup_frame"]
            markup_time = markup["markup_time"]
            markup_vector = markup["markup_vector"]
            markup_path = json.dumps(markup["markup_path"])
            
            # Вставка данных в таблицу markups
            # print("Executing SQL:", cursor.mogrify(insert_markup_query, (markup_id, markup_frame, markup_time, json.dumps(markup_vector), markup_path)).decode("utf-8"))
            cursor.execute(insert_markup_query, (markup_id, markup_frame, markup_time, json.dumps(markup_vector), markup_path))

            # cursor.execute(insert_markup_query, (markup_id, markup_frame, markup_time, json.dumps(markup_vector), markup_path))
            
            # Вставка связи между цепочкой и разметкой в таблицу markups_chains
            cursor.execute(insert_chain_markup_query, (chain_id, markup_id))

# Фиксация изменений и закрытие соединения
conn.commit()
cursor.close()
conn.close()
print(f'chains:{cnt_chains}, markups:{cnt_markups}')