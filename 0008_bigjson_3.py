import os
import dbquery as dbq
import json
import datetime
from sqlalchemy import text
import threading
from sqlalchemy.orm import Session

class responseMessage():

    def __init__(self):
        self.message = {'error': True, 'text': ''}

    def composeMessage(self, is_error, text):
        self.message['error'] = is_error
        self.message['text']= text

    def set(self, text):
        self.composeMessage(False, text)
    
    def setError(self, text):
        self.composeMessage(True, text)

    def get(self)->json:
        return self.message


class parseJson():
    def __init__(self, dir_json):
        self.message = responseMessage()
        self.parse_error = False
        self.start = str(datetime.datetime.now())
        self.end = ''
        self.dir_json = dir_json
        

    def setError(self, text):
        self.parse_error = True # error mark
        self.message.setError(text)\
        

    def resetError(self):
        self.parse_error = False # error mark
        self.message.set('')


    def thread_runner_batch(self, connection, stmt):
        try:
            with Session(connection) as session:
                session.execute( text(stmt) )
                session.commit()
        finally:
            # closes the connection, i.e. the socket etc.
            connection.close()
            

    def dbq_tread_mark_insert(self, chains):
        rows_cnt = 0
        for chain in chains:
            dbq.tread_mark_insert(json.dumps(chain['chain_markups']['markup_path']))
            rows_cnt += 1
        return rows_cnt

    def tread_mark_insert_batch(self, stmt):
        # connection from the regular pool
        engine = dbq.create_engine(dbq.get_connection_string())
        connection = engine.connect()
        # detach it! now this connection has nothing to do with the pool.
        connection.detach()
        # pass the connection to the thread.  
        threading.Thread(target=self.thread_runner_batch, args=(connection, stmt)).start()

    def insert_init(self):
        return """INSERT INTO public.markups( id, previous_id, dataset_id, file_id, parent_id, 
                        mark_time, mark_path, vector, description, author_id,  dt_created, is_deleted ) 
                  VALUES """

    def add_query_values(self, values):
        return f"(\'{values[0]}\', null, null, null, null, 99, \'{values[1]}\', \'{values[1]}\', \'tread\', "\
                f"null, \'2024-10-08 12:42:00\', false)"
    

    def insert_new(self, query_values):
        return self.insert_init() + ",".join(query_values)


    def ann_out_db_save(self, content):
        count_values = 0
        query_values = []
        query_size = 1000
        for f in content['files']:
            for chain in f['file_chains'] :
                for chain_markup in chain['chain_markups']:
                    mdata = json.dumps(chain_markup["markup_path"])
                    mid = dbq.getUuid()
                    file_id = dbq.getUuid()
                    query_values.append(self.add_query_values([mid, mdata, file_id]))
                    count_values += 1 
                    if(count_values % query_size == 0 ):
                        self.tread_mark_insert_batch( self.insert_new(query_values))
                        query_values.clear()
        if(len(query_values) > 0): # сохраняем значения из последнего набора, в котором кол-во строк меньше query_size
            self.tread_mark_insert_batch( self.insert_new(query_values))
           
        return self.message.get()
    

    def load_json(self, filepath):
        content = {}
        with open(filepath, "r") as file:
            try:
                content = json.load(file)
            except Exception as e:
                self.setError(f"Cannot load json from {file}")
        return content
    
    # обходим список json файлов
    def loop_files(self, files):
        cnt_saved = cnt_error = 0
        for filename in files:
            content = self.load_json(f"{self.dir_json}/{filename}")
            if( not self.parse_error):
                self.ann_out_db_save(content)
                cnt_saved += 1
            else:
                print(self.message.get()) # print error message and go to next file
                self.resetError()
                cnt_error += 1

        self.end =  str(datetime.datetime.now())
        self.message.set(f"Все файлы обработаны. 'start': {self.start}, 'end': {self.end} , "\
                         f"'results' : 'Всего:{len(files)} Успешно:{cnt_saved} Ошибок:{cnt_error}'" )


    def get_files(self, ext = []):
        files = []
        for fl in os.listdir(self.dir_json):
            if os.path.isfile(f'{self.dir_json}/{fl}') :
                f, ex = os.path.splitext(fl)
                if( len(ext) == 0 or ex[1:] in ext ):
                    files.append(fl)
        return files


    def mng_parse_ann_output(self):
        if not os.path.isdir(self.dir_json):
            self.message.setError("Ошибка: указанная директория не сущестует или не доступна")
        else:
            files = self.get_files(['json'])
            if len(files) == 0 : 
                self.message.setError("Ошибка: в указанной директории json файлы не найдены")
            else:
                self.loop_files(files)
                # self.message.set("Файлы запущены в обработку")

        return self.message.get()


if(__name__ == "__main__"):
    resp = parseJson(os.path.dirname(__file__)+'/json/0009')
    res = resp.mng_parse_ann_output()
    print(res)