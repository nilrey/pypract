import os
import random
import hashlib
import uuid
import threading
from datetime import datetime
import time


def get_new_hash(n):
    # print(uuid.UUID(hashlib.sha256().hexdigest()[::2]) )
    print(f"Start tread #{n}")
    time.sleep(1)
    print(uuid.UUID(
        hashlib.sha256(str( 
            random.random()+datetime.now().timestamp() 
         ).encode('utf-8')
        ).hexdigest()[::2]) )
    time.sleep(1)
    print(f"End tread #{n}")

if(__name__ == "__main__"):
    threading.Thread(target=get_new_hash, args=("10",) ).start()
    threading.Thread(target=get_new_hash, args=("11",) ).start()
