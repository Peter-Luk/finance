from alchemy import load, mapper
from utilities import web_collect
class RD(object): pass

class Equities(object):
    def __init__(self, *args):
        self.db_name = 'Securities'
        self.RD = RD
        if args:
            if isinstance(args[0], str): self.db_name = args[0]
        self.db = load(self.db_name)
        mapper(self.RD, self.db[self.db_name]['table'])
        self.session = self.db[self.db_name]['session']
        self.query = self.session.query(self.RD)
        avail_eid = [_[0] for _ in self.session.query(self.RD.eid).distinct()]
        avail_eid.sort()
        self.eid = [_ for _ in avail_eid if _ not in [805]]
        self.data_fields = ['open', 'high', 'low', 'close',  'volume']

    def __del__(self):
        self.db_name = self.db = self.session = self.query = self.eid = self.data_fields = None
        del(self.db_name, self.db, self.session, self.query, self.eid, self.data_fields)

    def check(self):
        i_count = 0
        try:
            wdata = web_collect(self.eid)
            for _ in self.eid:
                witem = wdata['{:04d}.HK'.format(_)]
                lwik = [_.to_pydatetime().date() for _ in witem.keys()]
                lwik.sort()
                sitemdate = [_[0] for _ in self.query.filter(self.RD.eid == _).values(self.RD.date)]
                for __ in lwik:
                    if __ not in sitemdate:
                        if self.query.filter(self.RD.eid == _, self.RD.date == __).value(self.RD.volume) != 0:
                            nr = self.RD()
                            nr.eid = _
                            nr.date = __
                            for f in self.data_fields:
                                exec('nr.{0} = witem[__][{0}.capitalize()]'.format(f))
                                if f == 'volume': exec('nr.{0} = int(witem[__][{0}.capitalize()])'.format(f))
                            self.session.add(nr)
                            self.session.commit()
                            i_count += 1
                            self.session.flush()
        except: pass
        return i_count
