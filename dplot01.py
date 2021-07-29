import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# import pandas as pd
# from datetime import datetime
from fat import Equity

# e1 = Equity('mrna', exchange='Nasdaq')
e1 = Equity(4523, exchange='TSE')
df = e1()
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
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03,
#                         subplot_titles=('OHLC', 'Volume'),
                        subplot_titles=(e1.yahoo_code, 'RSI', 'Volume'),
                        row_width=[0.15, 0.15, 0.6])

    # Plot OHLC on 1st row
    fig.add_trace(go.Candlestick(x=df.index, open=df["open"],
                                 high=df["high"], low=df["low"],
                                 close=df["close"], name="OHLC"),
                  row=1, col=1
                  )
    fig.add_trace(go.Scatter(x=df.index, y=e1.kama(), name='KAMA',
        line=dict(color='firebrick', width=1)), row=1, col=1)
    kc = e1.kc()
    fig.add_trace(go.Scatter(x=df.index, y=kc.Upper, name='Upper',
        line=dict(color='blue', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=kc.Lower, name='Lower',
        line=dict(color='blue', width=1)), row=1, col=1)

    fig.add_trace(go.Scatter(x=df.index, y=e1.rsi(),
        showlegend=False, line=dict(color='green', width=2)),
                        row=2, col=1)
    # Bar trace for volumes on 3rd row without legend
    fig.add_trace(go.Bar(x=df.index, y=df['volume'],
                        showlegend=False), row=3, col=1)

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
