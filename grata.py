import datetime
from time import sleep
from random import randint
from c2lite3e import wap

def fetch_data(half_day: bool=False) -> None:
    hour: int = 12 if half_day else 16
    while datetime.datetime.now().time() < datetime.time(hour, 25, 0):
        sleep(randint(2,5))
    return wap()

if __name__ == "__main__":
    td = input("Trade Duration 'F'ull/'H'alf (default: 'F'): ")
    match td:
        case 'h'| 'H':
            hd = True
        case _:
            hd = False
    # fetch_data(half_day=True)
    fetch_data(hd)
