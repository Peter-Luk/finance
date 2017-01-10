from sqlite3 import connect
from utilities import filepath
from statistics import mean

f_cur = connect(filepath('Futures')).cursor()
mf7 = f_cur.execute("SELECT date, session, open, high, low, close FROM records WHERE code='MHIF7' ORDER BY date ASC, session DESC").fetchall()

def atr(data, period=12):
    tr, count = [], 0
    tr.append([data[0][0], data[0][1], data[0][-3] - data[0][-2]])
    count += 1
    while count < len(data):
        if data[count][0] == data[count - 1][0]:
            if data[count][-3] > data[count - 1][-3]:ma = data[count][-3]
            else:ma = data[count - 1][-3]
            if data[count][-2] < data[count - 1][-2]:mi = data[count][-2]
            else:mi = data[count - 1][-2]
        else:
            if data[count][-3] > data[count - 1][-1]:ma, mi = data[count][-3], data[count - 1][-1]
            elif data[count][-2] < data[count - 1][-1]:mi, ma = data[count][-2], data[count - 1][-1]
        tr.append([data[count][0], data[count][1], ma - mi])
        count += 1 
    res, daterange, i, hdr = {}, [], 0, {}
    while i < len(tr):
        hdr[tr[i][0]] = tr[i][-1]
        if tr[i][0] not in daterange:daterange.append(tr[i][0])
        i += 1

    res[daterange[period - 1]] = mean([hdr[x] for x in daterange[:period]])
    for d in daterange[period:]:res[d] = (res[daterange[daterange.index(d) - 1]] * (period - 1) + hdr[d]) / period
    return res
