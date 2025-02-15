"""
запускать из вирутуальной среды: source venv/bin/activate
предварительно убедиться что установлены sqlalchemy psycopg2: pip install sqlalchemy psycopg2
проверить версию: pip show psycopg2
не использовать Run от VScode - не правильно интерпритирует среду запуска. Возможно можно настроить.
запускать командой: python 0021_db_camino_connect.py 
"""
from sqlalchemy import create_engine
from sqlalchemy import text
 
username = 'postgres'  # имя пользователя
password = 'postgres'  # пароль
host = 'localhost'  # хост (или IP контейнера, если нужно)
port = '5432'   
database = 'camino'   

# Создание строки подключения для SQLAlchemy
DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database}"

# Создание engine для подключения
engine = create_engine(DATABASE_URL)

# Выполнение запроса с использованием соединения
with engine.connect() as connection:
    result = connection.execute(text("SELECT COUNT(*) FROM markups"))
    count = result.scalar()  # Получение значения из первого столбца первой строки
    print(f"Количество записей в таблице 'markups': {count}")
