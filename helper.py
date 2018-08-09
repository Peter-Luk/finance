import sqlite3 as lite
from utilities import filepath
conn = lite.connect(filepath('Securities'))

def cdu(eid):
    cur = conn.cursor()
    ar = cur.execute('SELECT COUNT(date) FROM records WHERE eid={:d}'.format(eid)).fetchone()[0]
    ur = cur.execute('SELECT COUNT(DISTINCT date) FROM records WHERE eid={:d}'.format(eid)).fetchone()[0]
    if ar > ur: return True

def rdu(eid):
    cur = conn.cursor()
    tds = [_[0] for _ in cur.execute('SELECT DISTINCT date FROM records WHERE eid={:d}'.format(eid)).fetchall()]
    dl = []
    for td in tds:
        rdl = [_[0] for _ in cur.execute("SELECT id FROM records WHERE eid={:d} AND date='{}'".format(eid, td)).fetchall()]
        if len(rdl) > 1: dl.extend(rdl[1:])
    if dl:
        for d in dl:
            cur.execute("DELETE FROM records WHERE id={:d}".format(d))
    conn.commit()
    return '{:d} record(s) deleted'.format(len(dl))
