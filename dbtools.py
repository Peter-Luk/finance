from alchemy import load, mapper
from utilities import web_collect
class RD(object): pass

class Equities(object):
    def __init__(self, *args):
        self.db_name = 'Securities'
        if args:
            if isinstance(args[0], str): self.db_name = args[0]
        self.db = load(self.db_name)
        mapper(RD, self.db[self.db_name]['table'])
        self.session = self.db[self.db_name]['session']
        self.query = self.session.query(RD)
        avail_eid = [_[0] for _ in self.session.query(RD.eid).distinct()]
        avail_eid.sort()
        self.eid = [_ for _ in avail_eid if _ not in [805]]
        self.data_fields = ['open', 'high', 'low', 'close',  'volume']

    def __del__(self):
        self.db_name = self.db = self.session = self.query = self.eid = self.data_fields = None
        del(self.db_name, self.db, self.session, self.query, self.eid, self.data_fields)

    def check(self):
        try:
            wdata = web_collect(self.eid)
            for _ in self.eid:
                witem = wdata['{:04d}.HK'.format(_)]
                lwik = [_.to_datetime().date() for _ in witem.keys()]
                lwik.sort()
                sitemdate = [_[0] for _ in self.query.filter(RD.eid == _).values(RD.date)]
                for __ in lwik:
                    if __ not in sitemdate:
                        if self.query.filter(RD.eid == _, RD.date == __).value(RD.volume) != 0:
                            nr = RD()
                            nr.eid = _
                            nr.date = __
                            for f in self.data_fields:
                                exec('nr.{0} = witem[__][{0}.capitalize()]'.format(f))
                                if f == 'volume': exec('nr.{0} = int(witem[__][{0}.capitalize()])'.format(f))
                            self.session.add(nr)
                            self.session.commit()
                            self.session.flush()
        except: pass
