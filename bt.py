him = getattr(__import__('handy'),'him')
iml = [{'bokeh.embed':('components',),'bokeh.io':('output_notebook', 'output_file', 'show', 'save'),'bokeh.layouts':('gridplot',),'bokeh.plotting':('figure',)},({'bokeh.palettes':('Viridis3',)}, "case='capitalize'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']"%(_,_))

class LF(object):
    def __init__(self, *args, **kwargs):
        if args: self.code = args[0]
        if 'code' in kwargs.keys(): self.code = kwargs['code']
        try:
            self.fp = getattr(__import__('pt_2'),'PI')(self.code)
        except:pass

    def __del__(self):
        self.fp = self.code = None
        del(self.code)
        del(self.fp)

    def plot(self, *args, **kwargs):
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
#         if webpage:
        try:
            if embed: return components(grid)
            output_file(webpage)
            save(grid)
#         elif embed: return components(grid)
#         else:
        except:
            output_notebook()
            show(grid)
