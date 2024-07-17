from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from sqlalchemy import text, insert, update, select, delete
import uuid, json
from typing import List
from sqlalchemy import bindparam, text
from configparser import ConfigParser
import datetime as dt
import threading 

def load_config(filename='database.ini', section='postgresql'):
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


def get_connection_string():
   config = load_config()
   # cs = 'postgresql://postgres:postgres@127.0.0.1:5432/camino_db1'
   cs = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
   return cs


def make_session():
   engine = create_engine(get_connection_string())
   session_maker = sessionmaker(bind=engine)
   return session_maker
   

def db_conn():
   session_maker = make_session()
   db: Session = session_maker()
   return db


def getUuid():
   return str(uuid.uuid4())


def select_wrapper(stmt, params={}):
   resp = {}
   with db_conn() as session:
      resp = session.execute(stmt, params).mappings().all()
   return resp

def q_user_select_by_id(id):
   stmt = text("SELECT * FROM  common.users u  \
   WHERE u.id = :user_id")
   resp = select_wrapper(stmt, {"user_id" : id} )
   return resp


def q_user_select_by_id(id):
   stmt = text("SELECT * FROM  common.users u  \
   WHERE u.id = :user_id")
   resp = select_wrapper(stmt, {"user_id" : id} )
   return resp


def q_user_insert(user_name = 'user7'):
   stmt = text("INSERT INTO common.users( id, name, role_code, login, password, description, is_deleted) VALUES \
               (:user_id, :user_name, 'user1', 'user1', 'user1','user1', false);")

   with db_conn() as session:
      resp = session.execute(stmt, {"user_id": getUuid(), "user_name":user_name } )
      session.commit()
   return {}


def tread_mark_insert(mp, mv):
    # connection from the regular pool
    engine = create_engine(get_connection_string())
    connection = engine.connect()

    # detach it! now this connection has nothing to do with the pool.
    connection.detach()

    # pass the connection to the thread.  
    t = threading.Thread(target=my_thread_runner, args=(connection, mp, mv))
    t.start()

def my_thread_runner(connection, mp, mv):
    try:
        with Session(connection) as session:
            stmt = text("""
                        INSERT INTO common.markups( id, previous_id, dataset_id, file_id, parent_id, mark_time, mark_path, vector, description, author_id, dt_created, is_deleted)
                        VALUES (:item_id, null, null, null, null, 99, :mark_path, :mark_vector, 'tread', null, '2024-12-12 12:23:33', false);
                     """)
            resp = session.execute(stmt, {"item_id": getUuid(),
                                          "mark_path": mp ,
                                          "mark_vector" : mv } )
            session.commit()



    finally:
        # closes the connection, i.e. the socket etc.
        connection.close()


def q_mark_insert(mp, mv):
   stmt = text("""
               INSERT INTO common.markups( id, previous_id, dataset_id, file_id, parent_id, mark_time, 
               mark_path, vector, description, 
               author_id, dt_created, is_deleted)
               VALUES (:item_id, null, null, null, null, 99, 
               :mark_path, :mark_vector, 'test', 
               null, '2024-12-12 12:23:33', false);
             """)
   # stmt.bindparams(mark_path=mp)
   # print(stmt)
   with db_conn() as session:
      resp = session.execute(stmt, {"item_id": getUuid(), 
                                    "mark_path": mp ,
                                    "mark_vector" : mv } )
      session.commit()
   return True


if(__name__ == "__main__"):
   print(get_connection_string())
   print("Ok")
   # print(q_user_select_by_id('e6702a5d-13ec-41d5-aea4-c12a5af92e18'))
   q_user_insert()


   # INSERT INTO common.markups(
	# id, previous_id, dataset_id, file_id, parent_id, mark_time, mark_path, vector, description, author_id, dt_created, is_deleted)
	# VALUES ('e6702a5d-13ec-41d5-aea4-c12a5af92e18', 
	# 'e6702a5d-13ec-41d5-aea4-c12a5af92e18', 
	# 'e6702a5d-13ec-41d5-aea4-c12a5af92e18', 
	# 'e6702a5d-13ec-41d5-aea4-c12a5af92e18', 
	# 'e6702a5d-13ec-41d5-aea4-c12a5af92e18', 
	# 11, '[{"test":"test"}]', 'test', 'test', 'e6702a5d-13ec-41d5-aea4-c12a5af92e18', '2024-12-12 12:23:33', false);