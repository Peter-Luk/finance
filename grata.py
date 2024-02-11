import datetime
from time import sleep
from random import randint
from c2lite3e import wap

def fetch_data(hour, minute, second=0):
    while datetime.datetime.now().time() < datetime.time(hour, minute, second):
        sleep(randint(2,5))
    return wap()

if __name__ == "__main__":
    fetch_data(16, 25)
    # fetch_data(12, 25)
