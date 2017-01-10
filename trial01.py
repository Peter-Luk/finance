from sqlite3 import connect
from utilities import filepath
from statistics import mean

f_cur = connect(filepath('Futures')).cursor()
mf7 = f_cur.execute("SELECT date, session, open, high, low, close FROM records WHERE code='MHIF7' ORDER BY date ASC, session DESC").fetchall()

def atr(data, period=5):
    res, r_date, tr, i, hdr = {}, [], [], 0, {}
    tr.append([data[0][0], data[0][1], data[0][-3] - data[0][-2]])
    i += 1
    while i < len(data):
        if data[i][0] == data[i - 1][0]:
            if data[i][-3] > data[i - 1][-3]:ma = data[i][-3]
            else:ma = data[i - 1][-3]
            if data[i][-2] < data[i - 1][-2]:mi = data[i][-2]
            else:mi = data[i - 1][-2]
        else:
            if data[i][-3] > data[i - 1][-1]:ma, mi = data[i][-3], data[i - 1][-1]
            elif data[i][-2] < data[i - 1][-1]:mi, ma = data[i][-2], data[i - 1][-1]
        tr.append([data[i][0], data[i][1], ma - mi])
        i += 1 
    i = 0
    while i < len(tr):
        hdr[tr[i][0]] = tr[i][-1]
        if tr[i][0] not in r_date:r_date.append(tr[i][0])
        i += 1

    res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
    for d in r_date[period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period
    return res
