
# coding: utf-8

# In[1]:

from bokeh.io import output_notebook, show
from bokeh.layouts import gridplot
from bokeh.palettes import Viridis3
from bokeh.plotting import figure

code = 'hsik7'
cmatch = {'EMA':Viridis3[1], 'WMA':Viridis3[0], 'SMA':Viridis3[2], 'KAMA':'red'}
omatch = {'KC':'red', 'APZ':Viridis3[1], 'BB':Viridis3[2]}
mp = getattr(__import__('pt_2'),'PI')(code)
imp, omp, bmp = mp.fdc('i'), mp.fdc('o'), mp.fdc('b')
output_notebook()


# In[2]:

q = figure(title='%s RSI' % code.upper(), x_axis_label='Date', background_fill_color='#DFDFE5', plot_height=250, x_axis_type='datetime')
q.xgrid.grid_line_color = 'white'
#q.xgrid.grid_line_width = 2
q.ygrid.grid_line_color = 'white'
#q.ygrid.grid_line_width = 2
q.line(imp['Date'], imp['RSI'], legend='RSI', color='navy', line_width=3, alpha=0.5)
q.legend.location = 'top_left'

inc = bmp.Close > bmp.Open
dec = bmp.Open > bmp.Close
w = 12 * 60 * 60 * 1000
TOOLS = 'pan,wheel_zoom,box_zoom,reset,save'
r = figure(x_axis_type='datetime', tools=TOOLS, title='%s daily with Candlestick' % code.upper(), background_fill_color='#DFDFE5', x_range=q.x_range)
r.xgrid.grid_line_color = 'white'
r.xgrid.grid_line_width = 3
r.ygrid.grid_line_color = 'white'
r.ygrid.grid_line_width = 3
r.grid.grid_line_alpha = 0.3
r.segment(bmp.Date, bmp.High, bmp.Date, bmp.Low, color='black')
r.vbar(bmp.Date[inc], w, bmp.Open[inc], bmp.Close[inc], fill_color='blue', line_color='black')
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
show(grid)

