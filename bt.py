him = getattr(__import__('handy'), 'him')
iml = [{'bokeh.embed':('components',), 'bokeh.io':('output_notebook', 'output_file', 'show', 'save'), 'bokeh.layouts':('gridplot',), 'bokeh.plotting':('figure',), 'os':('linesep',)}, ({'bokeh.palettes':('Viridis3',)}, "case='capitalize'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']"%(_,_))

class LF(object):
    def __init__(self, *args, **kwargs):
        if args: self.code = args[0]
        if 'code' in kwargs.keys(): self.code = kwargs['code']
        try:
            self.fp = getattr(__import__('pt_2'),'PI')(self.code)
        except: pass

    def __del__(self):
        self.fp = self.code = None
        del(self.code)
        del(self.fp)

    def plot(self, *args, **kwargs):
# from bokeh.models.formatters import TickFormatter, String, List
# import pandas as pd

# class DateGapTickFormatter(TickFormatter):
#     date_labels = List(String)
#     __implementation__ = """
#         _ = require "underscore"
#         Model = require "model"
#         p = require "core/properties"

#         class DateGapTickFormatter extends Model
#             type: 'DateGapTickFormatter'

#             doFormat: (ticks) ->
#                 date_labels = @get("date_labels")
#                 return (date_labels[tick] ? "" for tick in ticks)

#             @define {
#                 date_labels: [ p.Any ]
#             }

#         module.exports =
#             Model: DateGapTickFormatter
# """

        embed = False
        if args:
            webpage = args[0]
        if 'embed' in kwargs.keys(): embed = kwargs['embed']
        if 'webpage' in kwargs.keys(): webpage = kwargs['webpage']
        if embed:
            webpage = None
        cmatch = {'EMA':Viridis3[1], 'WMA':Viridis3[0], 'SMA':Viridis3[2], 'KAMA':'red'}
        omatch = {'KC':'red', 'APZ':Viridis3[1], 'BB':Viridis3[2]}
        imp, omp, bmp = self.fp.fdc('i'), self.fp.fdc('o'), self.fp.fdc('b')

        q = figure(title='%s RSI' % self.code.upper(), x_axis_label='Date', background_fill_color='#DFDFE5', plot_height=250, x_axis_type='datetime')
        q.xgrid.grid_line_color = 'white'
        q.ygrid.grid_line_color = 'white'
        q.line(imp['Date'], imp['RSI'], legend='RSI', color='navy', line_width=3, alpha=0.5)
        q.legend.location = 'top_left'

        inc = bmp.Close > bmp.Open
        dec = bmp.Open > bmp.Close
        w = 12 * 60 * 60 * 1000
        TOOLS = 'pan,wheel_zoom,box_zoom,reset,save'
        r = figure(x_axis_type='datetime', tools=TOOLS, title='%s daily with Candlestick' % self.code.upper(), background_fill_color='#DFDFE5', x_range=q.x_range)
        r.xgrid.grid_line_color = 'white'
        r.xgrid.grid_line_width = 3
        r.ygrid.grid_line_color = 'white'
        r.ygrid.grid_line_width = 3
        r.grid.grid_line_alpha = 0.3
        r.segment(bmp.Date, bmp.High, bmp.Date, bmp.Low, color='black')
        r.vbar(bmp.Date[inc], w, bmp.Open[inc], bmp.Close[inc], fill_color='green', line_color='black')
        r.vbar(bmp.Date[dec], w, bmp.Open[dec], bmp.Close[dec], fill_color='red', line_color='black')
        for k, v in omatch.items():
            l, h = [], []
            if k == 'KC':
                for i in omp.KC.values:
                    l.append(i[0])
                    h.append(i[-1])
            if k == 'BB':
                for i in omp.BB.values:
                    l.append(i[0])
                    h.append(i[-1])
            if k == 'APZ':
                for i in omp.APZ.values:
                    l.append(i[0])
                    h.append(i[-1])
            r.line(omp['Date'], l, color=v, line_width=3, alpha=0.5)
            r.line(omp['Date'], h, color=v, legend=k, line_width=3, alpha=0.5)
        [r.circle(imp['Date'], imp[k], legend=k, color=v, size=5) for k, v in cmatch.items()]
        r.legend.location = 'top_left'
        grid = gridplot([r, q], ncols=1, plot_width=800)
        try:
            if embed: return components(grid)
            output_file(webpage)
            save(grid)
        except:
            output_notebook()
            show(grid)

    def ds(self, *args, **kwargs):
        print('%s: (latest @ %s)' % (self.code.upper(), self.fp.trade_day[-1]))
        try:
            mos = getattr(self.fp, 'ltdmos')('a')
            ar = getattr(self.fp, 'fdc')('b')
            dm, ds = ar['Delta'].mean(), ar['Delta'].std()
            lv, vm, vs = ar['Volume'].values[-1], ar['Volume'].mean(), ar['Volume'].std()
            lc, cs = ar['Close'].values[-1], ar['Close'].std()
            print('Close: %i' % lc)
            print("%sVolume over mean: %.2f%%" % (linesep, lv / vm* 100.))
            print("Volume over (mean + std): %.2f%%" % (lv / (vm +vs) * 100.))
            il = list(filter(lambda _:(_ > lc - cs) and (_ < lc + cs), mos))
            ol = list(filter(lambda _:(_ < lc - cs) or (_ > lc + cs), mos))
            print('%sWithin statistical range:' % linesep, il)
            ml = list(filter(lambda _:_ > lc, ol))
            csl = list(filter(lambda _:_ not in ml, ol))
            if ml: print('%sMoon shot:' % linesep, ml)
            if csl: print('China syndrome:', csl)
            xd = getattr(self.fp, 'xfinder')('d')
            dtxd = xd.transpose().to_dict()
            if len(dtxd.keys()):
                print('%sDelta extreme case:' % linesep)
                for _ in list(dtxd.keys()):
                    print("%s: %i (%i - %i)" % (dtxd[_]['Date'].strftime('%d-%m-%Y'), dtxd[_]['Delta'], dm - ds, dm + ds))
        except:pass
        try:
            ai = getattr(self.fp, 'fdc')('i')
            xr = getattr(self.fp, 'xfinder')('r')
            rm, rs = ai['RSI'].mean(), ai['RSI'].std()
            dtxr = xr.transpose().to_dict()
            if len(dtxr.keys()):
                print('RSI extreme case:')
                for _ in list(dtxr.keys()):
                    print("%s: %.3f (%.3f - %.3f)" % (dtxr[_]['Date'].strftime('%d-%m-%Y'), dtxr[_]['RSI'], rm - rs, rm + rs))
        except:pass
