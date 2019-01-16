# from sqlalchemy import create_engine, MetaData, Table
import pref
db = pref.sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from utilities import filepath, datetime
from os import sep, environ, listdir
from sys import platform

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

class AE(AS):
    def __init__(self, eid):
        ae = AS(pref.db['Equities']['name'])
        self.columns = ae.columns
        self.connect = ae.connect
        self.table = ae.table
        self.code = eid

    def __del__(self):
        self.columns = self.connect = self.table = self.code = None
        del(self.columns, self.connect, self.table, self.code)

    def append(self, values, conditions):
        hdr = {self.columns.eid:self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[f'self.columns.{_}'] = values[_]
        query = db.insert(self.table)
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
        query = db.delete(self.table).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def update(self, values, conditions):
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
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[f'self.columns.{_}'] = values[_]
        query = db.update(self.table).values(hdr).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

class AF(AS):
    def __init__(self, code):
        ae = AS(pref.db['Futures']['name'])
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
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[f'self.columns.{_}'] = values[_]
        query = db.insert(self.table)
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
        query = db.delete(self.table).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def update(self, values, conditions):
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
        query = db.update(self.table).values(hdr).where(self.columns.id==obtain_id(conditions))
        # return query
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()
