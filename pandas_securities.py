import pandas as pd
import sqlite3 as lite
from utilities import filepath
from pref import db

def fetch(db, table):
    conn = lite.connect(filepath(db))
    df = ('date', 'open', 'high', 'low', 'close', 'volume')
    bcol = ['eid as Code']
    bcol.extend([f'{_} as {_.capitalize()}' for _ in df])
    qstr = "SELECT " + ', '.join(bcol) + f" FROM {table} ORDER BY Code ASC, Date ASC"
    _ = pd.read_sql_query(qstr, conn, index_col=['Code', 'Date'], parse_dates=['Date'])
    _.sort_index()
    return _

dbname = db['Equities']['name']
table = db['Equities']['table']
data = fetch(dbname, table)
