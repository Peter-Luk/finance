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
        self.RD = self.db_name = self.db = self.session = self.query = self.eid = self.data_fields = None
        del(self.RD, self.db_name, self.db, self.session, self.query, self.eid, self.data_fields)

    def check(self):
        res, u_count, i_count = '', 0, 0
        try:
            wdata = web_collect(self.eid)
            for _ in self.eid:
                witem = wdata['{:04d}.HK'.format(_)]
                lwik = [_.to_pydatetime().date() for _ in witem.keys()]
                lwik.sort()
                sitemdate = [_[0] for _ in self.query.filter(self.RD.eid == _).values(self.RD.date)]
                for __ in lwik:
                    vol = witem[__]['Volume']
                    if __ in sitemdate:
                        dhdr, iitem = {}, self.query.filter(self.RD.eid == _, self.RD.date == __)
                        if vol != 0:
                            # dhdr['eid'] = _
                            # dhdr['date'] = __
                            if iitem.value(self.RD.open) != witem[__]['Open']: dhdr['open'] = witem[__]['Open']
                            if iitem.value(self.RD.high) != witem[__]['High']: dhdr['high'] = witem[__]['High']
                            if iitem.value(self.RD.low) != witem[__]['Low']: dhdr['low'] = witem[__]['Low']
                            if iitem.value(self.RD.close) != witem[__]['Close']: dhdr['close'] = witem[__]['Close']
                            if iitem.value(self.RD.volume) != vol: dhdr['volume'] = vol
                        if dhdr:
                            iitem.update(dhdr)
                            self.session.commit()
                            u_count += 1
                            self.session.flush()
                    else:
                        if vol != 0:
                            nr = self.RD()
                            nr.eid = _
                            nr.date = __
                            nr.volume = int(vol)
                            for f in [_ for _ in self.data_fields if _ not in ['volume']]:
                                exec('nr.{0} = witem[__][{0}.capitalize()]'.format(f))
                            self.session.add(nr)
                            self.session.commit()
                            i_count += 1
                            self.session.flush()
        except: pass
        if i_count:
            if u_count: res = '{:d} append, {:d} amend'.format(i_count, u_count)
            res = '{:d} append'.format(i_count)
        elif u_count: res = '{:d} amend'.format(u_count)
        if res: return res
