import threading
import time


class Data():
    def __init__(self, project_id) -> None:
        self.project_id = project_id


class Test():

    def __init__(self):
        self.ids= {"id" : ""}
        self.data = ""

    def start(self, val, t):
        self.ids["id"] = val
        time.sleep(t)
        print(self.ids["id"])

    def start_with_data(self, val, t):
        self.data = val
        time.sleep(t)
        print(self.data.project_id)


    def thread_proc(self):
        # threading.Thread(target=self.start, args=("a1",2)).start()
        # threading.Thread(target=self.start, args=("b1",1)).start()
        d1 = Data("a1")
        threading.Thread(target=self.start_with_data, args=(d1,2)).start()
        d2 = Data("b1")
        threading.Thread(target=self.start_with_data, args=(d2,1)).start()

if (__name__ == "__main__"):
    test1 = Test()
    # test2 = Test()
    # threading.Thread(target=test1.start, args=("a1",3)).start()
    # threading.Thread(target=test2.start, args=("b1",1)).start()
    test1.thread_proc()
