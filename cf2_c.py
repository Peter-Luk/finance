import cherrypy
from bt import LF as lf
from utilities import datetime, linesep, platform, ltd, waf, today, IP
from sys import argv, version_info
from tags import HTML, HEAD, TITLE, BODY, FORM, TABLE, TR, TD, LABEL, SELECT, OPTION, BUTTON, INPUT
panda = False
try:
    PI = getattr(__import__('pt_2'),'PI')
    panda = True
except:
    summary = getattr(__import__('trial01'), 'summary')

server_host, server_port = str(IP()), 80
# server_host, server_port = IP('public').address, 80
if len(argv) > 1: server_host = argv[1]
if platform in ('linux', 'linux2'): server_port = 2080
cherrypy.config.update({'server.socket_host': server_host,'server.socket_port': server_port})

class Analysor(object):
    @cherrypy.expose
    def index(self):
        hd = HEAD(TITLE('Analyse records'))
        ops = [OPTION(_, {'value':_}) for _ in waf()]
        if today.day == ltd(today.year, today.month): ops = [OPTION(_, {'value':_}) for _ in waf(1)]
        if version_info.major == 3:
            if version_info.minor > 6:
                sl = SELECT(linesep.join([f'{_}' for _ in ops]), {'name':'contract'})
                btn = BUTTON('Analyse', {'type':'submit'})
                trs = [TR(linesep.join([f'{_}' for _ in [TD(LABEL('Contract: ')), TD(linesep.join([f'{__}' for __ in [sl,btn]]), {'align':'right'})]]))]
                bd = BODY(FORM(TABLE(linesep.join([f'{_}' for _ in trs])), {'method':'post', 'action':'proceed'}))
                return str(HTML(linesep.join([f'{_}' for _ in [hd,bd]])))
            else:
                sl = SELECT(linesep.join(['{}'.format(_) for _ in ops]), {'name':'contract'})
                btn = BUTTON('Analyse', {'type':'submit'})
                trs = [TR(linesep.join(['{}'.format(_) for _ in [TD(LABEL('Contract: ')), TD(linesep.join(['{}'.format(__) for __ in [sl,btn]]), {'align':'right'})]]))]
                bd = BODY(FORM(TABLE(linesep.join(['{}'.format(_) for _ in trs])), {'method':'post', 'action':'proceed'}))
                return str(HTML(linesep.join(['{}'.format(_) for _ in [hd,bd]])))

    @cherrypy.expose
    def proceed(self, contract):
        try:
            s, d = lf(contract.upper()).plot(embed=True)
            bbase = "http://cdn.pydata.org/bokeh/release/bokeh-%s.min" % bv
            bscript = '<script type="text/javascript" scr="%s"></script>' % '.'.join((bbase, 'js'))
            blink = '<link href="%s.css" rel="stylesheet" type="text/css" />' % bbase
            blink += bscript + '<script type="text/javascript"> Bokeh.set_log_level="info"; </script>%s' % s
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
        ops = [OPTION(_, {'value':_}) for _ in waf()]
        if today.day == ltd(today.year, today.month): ops = [OPTION(_, {'value':_}) for _ in waf(1)]
        sl = SELECT(linesep.join([f'{_}' for _ in ops]), {'name':'contract'})
        btn = BUTTON('Append', {'type':'submit'})
        if version_info.major == 3:
            if version_info.minor > 6:
                sl = SELECT(linesep.join([f'{_}' for _ in ops]), {'name':'contract'})
                btn = BUTTON('Append', {'type':'submit'})
                trs = [TR(linesep.join([f'{_}' for _ in [TD(LABEL('Contract: ')), TD(linesep.join([f'{__}' for __ in [sl, btn]]), {'align':'right'})]]))]
                trs.extend([TR(linesep.join([f'{_}' for _ in [TD(f'{__.capitalize()}', {'align':'right'}), TD(INPUT({'type':'text', 'name':__}))]])) for __ in ['open', 'high', 'low', 'close', 'volume']])
                bd = BODY(FORM(TABLE(linesep.join([f'{_}' for _ in trs])), {'method':'post','action':'append'}))
                return str(HTML(linesep.join([f'{_}' for _ in [hd, bd]])))
            else:
                sl = SELECT(linesep.join(['{}'.format(_) for _ in ops]), {'name':'contract'})
                btn = BUTTON('Append', {'type':'submit'})
                trs = [TR(linesep.join(['{}'.format(_) for _ in [TD(LABEL('Contract: ')), TD(linesep.join(['{}'.format(__) for __ in [sl, btn]]), {'align':'right'})]]))]
                trs.extend([TR(linesep.join(['{}'.format(_) for _ in [TD(f'{__.capitalize()}', {'align':'right'}), TD(INPUT({'type':'text', 'name':__}))]])) for __ in ['open', 'high', 'low', 'close', 'volume']])
                bd = BODY(FORM(TABLE(linesep.join(['{}'.format(_) for _ in trs])), {'method':'post','action':'append'}))
                return str(HTML(linesep.join(['{}'.format(_) for _ in [hd, bd]])))

    @cherrypy.expose
    def append(self, contract, open, high, low, close, volume):
#         from datetime import datetime
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
        ops = [OPTION(_, {'value':_}) for _ in waf()]
        if today.day == ltd(today.year, today.month): ops = [OPTION(_, {'value':_}) for _ in waf(1)]
        if version_info.major == 3:
            if version_info.minor > 6:
                sl = SELECT(linesep.join([f'{_}' for _ in ops]), {'name':'contract'})
                btn = BUTTON('Estimate', {'type':'submit'})
                trs = [TR(linesep.join([f'{_}' for _ in [TD(LABEL('Contract: ')), TD(linesep.join([f'{__}' for __ in [sl, btn]]),{'align':'right'})]]))]
                trs.append(TR(linesep.join([f'{_}' for _ in [TD('Pivot Point', {'align':'right'}), TD(INPUT({'type':'text','name':'pp'}))]])))
                bd = BODY(FORM(TABLE(linesep.join([f'{_}' for _ in trs])), {'method':'post','action':'proceed'}))
                return str(HTML(linesep.join([f'{_}' for _ in [hd,bd]])))
            else:
                sl = SELECT(linesep.join(['{}'.format(_) for _ in ops]), {'name':'contract'})
                btn = BUTTON('Estimate', {'type':'submit'})
                trs = [TR(linesep.join(['{}'.format(_) for _ in [TD(LABEL('Contract: ')), TD(linesep.join(['{}'.format(__) for __ in [sl, btn]]),{'align':'right'})]]))]
                trs.append(TR(linesep.join(['{}'.format(_) for _ in [TD('Pivot Point', {'align':'right'}), TD(INPUT({'type':'text','name':'pp'}))]])))
                bd = BODY(FORM(TABLE(linesep.join(['{}'.format(_) for _ in trs])), {'method':'post','action':'proceed'}))
                return str(HTML(linesep.join(['{}'.format(_) for _ in [hd,bd]])))

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
