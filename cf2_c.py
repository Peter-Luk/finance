e = getattr(__import__('handy'),'encoder')
__ = e({'os':('linesep',),'sys':('argv','platform'),'utilities':('ltd','waf','today')})
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))
__ = e({'utilities':('IP',),'tags':('HTML', 'HEAD', 'TITLE', 'LINK', 'BODY', 'FORM', 'TABLE', 'TR', 'TD', 'LABEL', 'SELECT', 'OPTION', 'BUTTON', 'INPUT')}, case='upper')
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))
# from os import linesep
# from sys import argv, platform
# from tags import HTML, HEAD, TITLE, LINK, BODY, FORM, TABLE, TR, TD, LABEL, SELECT, OPTION, BUTTON, INPUT
# from utilities import ltd, today, waf, IP
panda = False
try:
    from pt_2 import PI
    panda = True
except:
    from trial01 import summary

import cherrypy

server_host, server_port = IP('public').address, 80
if len(argv) > 1: server_host = argv[1]
if platform in ('linux', 'linux2'): server_port = 2080
cherrypy.config.update({'server.socket_host': server_host,'server.socket_port': server_port})

class Analysor(object):
    @cherrypy.expose
    def index(self):
        hd = HEAD(TITLE('Analyse records'))
        ops = [OPTION(v, {'value':v}) for v in waf()]
        if today.day == ltd(today.year, today.month): ops = [OPTION(v, {'value':v}) for v in waf(1)]
        sl = SELECT(linesep.join([str(v) for v in ops]), {'name':'contract'})
        btn = BUTTON('Analyse', {'type':'submit'})
        trs = [TR(linesep.join([str(v) for v in [TD(LABEL('Contract: ')), TD(linesep.join([str(u) for u in [sl,btn]]), {'align':'right'})]]))]
        bd = BODY(FORM(TABLE(linesep.join([str(v) for v in trs])), {'method':'post', 'action':'proceed'}))
        return str(HTML(linesep.join([str(v) for v in [hd,bd]])))

    @cherrypy.expose
    def proceed(self, contract):
#         if panda:
        try:
            bv = getattr(__import__('bokeh'), '__version__')
            gp = getattr(__import__('bokeh_trial'),'genplot')
            s, d = gp(code=contract.upper(), embed=True)
            bbase = "http://cdn.pydata.org/bokeh/release/bokeh-%s.min" % bv
            bscript = '<script type="text/javascript" scr="%s"></script>' % '.'.join((bbase, 'js'))
            blink = linesep.join(('<link href="%s.css" rel="stylesheet" type="text/css" />' % bbase, bscript, '<script type="text/javascript"> Bokeh.set_log_level="info"; </script>', s))
            hd = HEAD(linesep.join(['<meta charset="utf-8">', str(TITLE(contract)), blink]))
            bd = BODY(d)
            return str(HTML(linesep.join([str(v) for v in [hd, bd]])))
        except:
            if panda:
                i2 = PI(code=contract)
                opt_value = 'B'
                if len(i2.trade_day) > i2.period: opt_value = 'I'
                return PI(code=contract).fdc(option=opt_value).to_html()
            return summary(code=contract, format='html')

class Inputter(object):
    @cherrypy.expose
    def index(self):
        hd = HEAD(TITLE('Daily statistic recording'))
        ops = [OPTION(v, {'value':v}) for v in waf()]
        if today.day == ltd(today.year, today.month): ops = [OPTION(v, {'value':v}) for v in waf(1)]
        sl = SELECT(linesep.join([str(v) for v in ops]), {'name':'contract'})
        btn = BUTTON('Append', {'type':'submit'})
        trs = [TR(linesep.join([str(x) for x in [TD(LABEL('Contract: ')), TD(linesep.join([str(v) for v in [sl, btn]]), {'align':'right'})]]))]
        trs.extend([TR(linesep.join([str(v) for v in [TD('%s: '%u.capitalize(), {'align':'right'}), TD(INPUT({'type':'text', 'name':u}))]])) for u in ['open', 'high', 'low', 'close', 'volume']])
        bd = BODY(FORM(TABLE(linesep.join([str(v) for v in trs])), {'method':'post','action':'append'}))
        return str(HTML(linesep.join([str(v) for v in [hd, bd]])))

    @cherrypy.expose
    def append(self, contract, open, high, low, close, volume):
        from datetime import datetime
        today, session = datetime.today(), 'M'
        hour, minute = today.hour, today.minute
        if hour > 12: session = 'A'
        elif (hour == 12) and (minute > 56): session = 'A'
        i2 = PI(code=contract)
        i2.append(session=session, open=open, close=close, high=high, low=low, volume=volume)
        if panda:
            opt_value = 'B'
            if len(i2.trade_day) > i2.period: opt_value = 'I'
            return PI(code=contract).fdc(option=opt_value).to_html()
        return summary(code=contract, format='html')

class Estimator(object):
    @cherrypy.expose
    def index(self):
        hd = HEAD(TITLE('Estimate session range'))
        ops = [OPTION(v, {'value':v}) for v in waf()]
        if today.day == ltd(today.year, today.month): ops = [OPTION(v, {'value':v}) for v in waf(1)]
        sl = SELECT(linesep.join([str(v) for v in ops]), {'name':'contract'})
        btn = BUTTON('Estimate', {'type':'submit'})
        trs = [TR(linesep.join([str(v) for v in [TD(LABEL('Contract: ')), TD(linesep.join([str(u) for u in [sl, btn]]),{'align':'right'})]]))]
        trs.append(TR(linesep.join([str(v) for v in [TD('Pivot Point', {'align':'right'}), TD(INPUT({'type':'text','name':'pp'}))]])))
        bd = BODY(FORM(TABLE(linesep.join([str(v) for v in trs])), {'method':'post','action':'proceed'}))
        return str(HTML(linesep.join([str(v) for v in [hd,bd]])))

    @cherrypy.expose
    def proceed(self, contract, pp):
        i2 = PI(code=contract)
        return i2.estimate(pivot_point=pp, format='html', concise=True)

if __name__ == '__main__':
    cherrypy.tree.mount(Inputter())
    cherrypy.tree.mount(Estimator(), '/estimate')
    cherrypy.tree.mount(Analysor(), '/analyse')
    cherrypy.engine.start()
    cherrypy.engine.block()
