import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import pandas as pd
from datetime import datetime
from fat import Equity

df = Equity('mrna', exchange='Nasdaq')()
# df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Checklist(
        id='toggle-rangeslider',
        options=[{'label': 'Include Rangeslider', 
                  'value': 'slider'}],
        value=['slider']
    ),
    dcc.Graph(id="graph"),
])

@app.callback(
    Output("graph", "figure"), 
    [Input("toggle-rangeslider", "value")])
def display_candlestick(value):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.03, subplot_titles=('OHLC', 'Volume'), 
        row_width=[0.2, 0.7])
    
    # Plot OHLC on 1st row
    fig.add_trace(go.Candlestick(x=df["date"], open=df["open"], high=df["high"],
                    low=df["low"], close=df["close"], name="OHLC"), 
                    row=1, col=1
    )
    
    # Bar trace for volumes on 2nd row without legend
    fig.add_trace(go.Bar(x=df['date'], y=df['volume'], showlegend=False), row=2, col=1)

#     fig = go.Figure(go.Candlestick(
#         x=df['date'],
#         open=df['open'],
#         high=df['high'],
#         low=df['low'],
#         close=df['close']
#         x=df['Date'],
#         open=df['AAPL.Open'],
#         high=df['AAPL.High'],
#         low=df['AAPL.Low'],
#         close=df['AAPL.Close']
#     ))

    fig.update_layout(
       xaxis_rangeslider_visible='slider' in value
    )

    return fig

app.run_server(debug=True)
