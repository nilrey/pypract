import threading
import time


class Data():
    def __init__(self, project_id) -> None:
        self.project_id = project_id


class Test():

    def __init__(self):
        self.data = "" 

    def start_with_data(self, val, t):
        data = val
        time.sleep(t)
        print(data.project_id)


    def thread_proc(self): 
        d1 = Data("00")
        d1.project_id = "a1"
        threading.Thread(target=self.start_with_data, args=(d1,2)).start()
        d2 = Data("00")
        d2.project_id = "b1"
        threading.Thread(target=self.start_with_data, args=(d2,1)).start()

if (__name__ == "__main__"):
    test1 = Test() 
    test1.thread_proc()
