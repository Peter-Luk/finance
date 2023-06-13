him = getattr(__import__('handy'), 'him')
__ = him({'datetime':('datetime',),'utilities':('today', 'filepath'), 'ormlib':('mtf'), 'os':('sep',), 'bt':('LF',)})
for _ in list(__.keys()): exec(f"{_}=__['{_}']")

def draw(_):
    trday, dstr = lf(_).fp.trade_day, ''
    pdate = datetime.strptime(trday[-1], '%Y-%m-%d').date()
    if (today.date() in [datetime.strptime(__, '%Y-%m-%d').date() for __ in trday]):
        pdate = today.date()
    if today.hour < 13: dstr = 'm'
    eval("getattr(lf('%s'), 'plot')(filepath('%sbt%02i%s.html', type=sep.join(('data', 'plot')), subpath=sep.join(('%i','%s'))))" % (_, _[0].lower(), pdate.day, dstr, pdate.year, pdate.strftime('%B')))
    st = 'full'
    if dstr == 'm': st = 'morning'
    return f"{_} ({pdate.strftime('%d-%m-%Y')} {st} session) plotted."

if __name__ == "__main__":
    for f in mtf():
        print(draw(f))
