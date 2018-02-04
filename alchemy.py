from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from os import sep, environ, listdir
from sys import platform
from utilities import datetime, mtf

if platform == 'win32': home = (''.join([environ['HOMEDRIVE'], environ['HOMEPATH']]))
if platform in ['linux', 'linux2']:
    subpath = 'shared'
    try:
        ext = environ['EXTERNAL_STORAGE']
        subpath = 'external-1'
    except: pass
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
        engine = create_engine('sqlite:///{}'.format(sep.join((home,) + db_path + (dbn,))))
        metadata = MetaData(engine)
        records = Table('records', metadata, autoload=True)
        eval('mapper({}, records)'.format(dbn))
#        mapper(Records, records)
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

def get_stored_eid(*args):
    db_name = 'Securities'
    if args:
        if isinstance(args[0], str): db_name = args[0]
        db = load(db_name)
    return [_[0] for _ in db[db_name]['engine'].execute("SELECT DISTINCT eid FROM records ORDER BY eid ASC").fetchall()]

def trade_date(*args):
    db_name = 'Futures'
    if args:
        if isinstance(args[0], str): code = args[0]
        if isinstance(args[1], str): db_name = args[1]
        db = load(db_name)
    ci = '{}R'.format(db_name[0])
    exec("class {}(object): pass".format(ci))
    exec("mapper({}exec, db[db_name]['table'])".format(ci))
    fq = eval("db[db_name]['session'].query({})".format(ci))
    res = []
    try:
        for _ in fq.all():
            if _.date not in res: res.append(_.date)
    except: pass
    return res

