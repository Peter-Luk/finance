# from sqlalchemy import create_engine, MetaData, Table
import pref
db, pd, datetime = pref.sqlalchemy, pref.pandas, pref.datetime
from sqlalchemy.orm import mapper, sessionmaker
from utilities import filepath
from os import sep, environ, listdir
from sys import platform
from nta import Viewer

eb, fb = pref.db['Equities'], pref.db['Futures']
if platform == 'win32': home = (''.join([environ['HOMEDRIVE'], environ['HOMEPATH']]))
if platform in ['linux', 'linux2']:
    subpath = 'shared'
    if environ['EXTERNAL_STORAGE']: subpath = 'external-1'
    home = sep.join([environ['HOME'], 'storage', subpath])

def get_db(*args, **kwargs):
    if isinstance(args[0], str): db_path = sep.join([home, args[0]])
    if isinstance(args[0], tuple): db_path = sep.join((home,) + args[0])
    if isinstance(args[0], list):
        args[0].insert(0, home)
        db_path = sep.join(args[0])
    if kwargs:
        try:
            if isinstance(kwargs['exclude'], str): exclude = [kwargs['exclude']]
            if isinstance(kwargs['exclude'], tuple): exclude = list(kwargs['exclude'])
            if isinstance(kwargs['exclude'], list): exclude = kwargs['exclude']
            if exclude: return [f for f in listdir(db_path) if not ('.git' in f or f in exclude)]
        except: pass
    return [f for f in listdir(db_path) if '.git' not in f]

def load(*args):
    res, db_path, exclude = {}, ('data','sqlite3'), 'Sec12'
    db_name = get_db(db_path, exclude=exclude)
    if args:
        if isinstance(args[0], str): db_name = [args[0]]
        if isinstance(args[0], list): db_name = args[0]
        if isinstance(args[0], tuple): db_name = list(args[0])
    for dbn in db_name:
        exec('class {}(object): pass'.format(dbn))
        engine = db.create_engine('sqlite:///{}'.format(sep.join((home,) + db_path + (dbn,))))
        metadata = db.MetaData(engine)
        records = db.Table('records', metadata, autoload=True)
        eval('mapper({}, records)'.format(dbn))
        Session = sessionmaker(bind=engine)
        session = Session()
        if dbn in ['Futures', 'Securities']:
            ifs = ['date', 'open', 'high', 'low', 'close', 'volume']
            if dbn == 'Futures':
                [ifs.insert(0, _) for _ in ['session', 'code']]
            else:
                [ifs.insert(0, _) for _ in ['eid']]
        else: ifs = ['subject_id', 'date', 'time', 'sys', 'dia', 'pulse', 'remarks']
        ifs = ['.'.join([dbn, _]) for _ in ifs]
        data = eval("session.query({}).all()".format(','.join(ifs)))
        res[dbn] = {'engine':engine, 'table':records, 'session':session, 'data':data}
    return res

class AS(object):
    def __init__(self, name):
        self.db = name
        engine = db.create_engine(f"sqlite:///{filepath(self.db)}")
        self.__meta = db.MetaData()
        if self.db == fb['name']:
            self.table = db.Table(fb['table'], self.__meta, autoload=True, autoload_with=engine)
        else:
            self.table = db.Table(eb['table'], self.__meta, autoload=True, autoload_with=engine)
        self.columns = self.table.columns
        self.connect = engine.connect()

    def __del__(self):
        self.__meta = self.table = self.columns = self.connect = None
        del(self.__meta, self.table, self.columns, self.connect)

class AE(Viewer, AS):
    def __init__(self, eid):
        pE = pref.db['Equities']
        ae = AS(pE['name'])
        self.columns = ae.columns
        self.connect = ae.connect
        self.table = ae.table
        self.code = eid
        self.data = self.acquire({'date':'>datetime(datetime.today().year - 4, 12, 31).date()'})
        self.close = self.data['Data'][-1, -2]
        self.view = Viewer(self.data)
        self.yahoo_code = f'{eid:04d}.HK'

    def __del__(self):
        self.columns = self.connect = self.table = self.code = self.data = self.close = self.view = self.yahoo_code = None
        del(self.columns, self.connect, self.table, self.code, self.data, self.close, self.view, self.yahoo_code)

    def append(self, values, conditions):
        hdr = {self.columns.eid:self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.insert()
        trans = self.connect.begin()
        self.connect.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['eid'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    if _ == 'date': hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        query = self.table.delete().where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def amend(self, values, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['eid'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    if _ == 'date': hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.update().values(hdr).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def acquire(self, conditions, dataframe=False):
        hdr = {'eid':f'=={self.code}'}
        for _ in conditions.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = conditions[_]
        query = db.select([self.columns.date, self.columns.open, self.columns.high, self.columns.low, self.columns.close, self.columns.volume]).where(eval('db.and_(' + ', '.join([f"self.columns.{_}{hdr[_]}" for _ in hdr.keys()]) + ')'))
        res = pd.DataFrame(self.connect.execute(query).fetchall(), columns=[__.capitalize() for __ in [f'{_}'.split('.')[-1] for _ in self.columns] if __ not in ['eid', 'id']])
        res.set_index('Date', inplace=True)
        if dataframe: return res
        return {'Date': list(res.index), 'Data': res.values}

    def ovr(self, raw=None, period=pref.periods['Equities'], date=datetime.today().date()):
        if not raw: raw = self.data
        return self.view.ovr(raw, period, date)

    def ratr(self, raw=None, period=pref.periods['Equities']['atr']):
        if not raw: raw = self.data
        return self.view.ratr(raw, period)

class AF(AS):
    def __init__(self, code):
        pF = pref.db['Futures']
        ae = AS(pF['name'])
        self.columns = ae.columns
        self.connect = ae.connect
        self.table = ae.table
        self.code = code

    def __del__(self):
        self.columns = self.connect = self.table = self.code = None
        del(self.columns, self.connect, self.table, self.code)

    def append(self, values, conditions):
        hdr = {self.columns.code:self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.insert()
        trans = self.connect.begin()
        self.connect.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    if _ == 'date': hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        query = self.table.delete().where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def amend(self, values, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    if _ in ['date', 'code', 'session']: hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.update().values(hdr).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()
