import dash_core_components as dcc
import dash_html_components as html
import numpy as np
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from django_plotly_dash import DjangoDash
from plotly.subplots import make_subplots
from pymongo import MongoClient
import pandas as pd
import talib
import numpy

client_mongo = MongoClient('localhost', 27017)
db = client_mongo['MarketData']
list_collections = db.list_collection_names()
options = []
for collection in list_collections:
    options.append({'label': collection, 'value': collection})

app = DjangoDash('CandlePrice', external_stylesheets=['/static/css/dash.css'])

app.layout = html.Div([
    html.Div([dcc.Dropdown(
        id='toggle-rangeslider',
        options=options,
        value='BTCUSDT', style={'width': '100%'}
    ), dcc.Dropdown(
        id='toggle',
        options=options,
        value='BTCUSDT', style={'width': '100%'})], style={'display': 'flex', 'width': '50%'}),
    dcc.Graph(id="graph",
              style={"backgroundColor": "#1a2d46", 'color': '#ffffff', 'height': '650px'},
              config=dict({'scrollZoom': True}))
], )


@app.callback(
    Output("graph", "figure"),
    [Input("toggle-rangeslider", "value")])
def display_candlestick(value):
    close_list = list(db[value].find({"timeframe": '5m'}))[0]
    df = pd.DataFrame()
    df['timestamp'] = close_list['time']
    df['date'] = pd.to_datetime(df['timestamp'], origin='unix', unit='ms')
    x = df['date'][-2000:]
    y = close_list['close'][-2000:]
    print(np.array(y))
    fig = make_subplots(2, 1)
    fig.add_trace(go.Scatter(
        x=x,
        y=y,
        line=dict(color='#1f77b4')
    ), 1, 1
    )
    fig.add_trace(go.Scatter(
        x=x,
        y=talib.SMA(np.array(y).astype('float64'), 50),
    ), 1, 1
    )
    fig.add_trace(go.Scatter(
        x=x,
        y=talib.RSI(np.array(y).astype('float64'), 50),
    ), 2, 1
    )
    fig.update_layout(
        autosize=True,
        paper_bgcolor='#27293d',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            rangeslider=dict(
                visible=False,

            ),
            gridcolor="#413b61"
        ),
        font=dict(color='white'),
        yaxis=dict(
            anchor="free",
            overlaying="y",
            side="right",
            position=1,
            gridcolor="#413b61"
        ),
        margin=dict(l=30, r=30, t=30, b=30),
    )
    fig.update_xaxes(matches='x')
    return fig
