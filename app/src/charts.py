import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tick_database import QuoteFields
from enum import Enum

class Chart:
    class ChartType(Enum):
        OHLC = "ohlc"
        CANDLESTICK = "candlestick"
    
    def __init__(self, title: str = "Chart", rangeslider: bool = False):
        self.title = title
        self.rangeslider = rangeslider
        self.main_data = None
        self.timestamp_col = None
        self.chart_type = self.ChartType.OHLC
        self.sub_panes = []
        self.fig = None
    
    def data(self, data: pd.DataFrame, timestamp_col: str = QuoteFields.time.value):
        self.main_data = data
        self.timestamp_col = timestamp_col
        return self
    
    def chart_type(self, chart_type: ChartType):
        self.chart_type = chart_type
        return self
    
    def add_sub_pane(self, data: pd.DataFrame, name: str = None):
        self.sub_panes.append({"data": data, "name": name})
        return self
    
    def show(self):
        subplot_count = 1 + len(self.sub_panes)
        self.fig = make_subplots(rows=subplot_count, cols=1, shared_xaxes=True)
        
        # Add main chart
        if self.chart_type == self.ChartType.CANDLESTICK:
            self.fig.add_trace(go.Candlestick(
                x=self.main_data[self.timestamp_col],
                open=self.main_data[QuoteFields.open.value],
                high=self.main_data[QuoteFields.high.value],
                low=self.main_data[QuoteFields.low.value],
                close=self.main_data[QuoteFields.close.value]
            ), row=1, col=1)
        else:  # OHLC
            self.fig.add_trace(go.Ohlc(
                x=self.main_data[self.timestamp_col],
                open=self.main_data[QuoteFields.open.value],
                high=self.main_data[QuoteFields.high.value],
                low=self.main_data[QuoteFields.low.value],
                close=self.main_data[QuoteFields.close.value]
            ), row=1, col=1)
        
        # Add sub-panes
        for i, sub_pane in enumerate(self.sub_panes, start=2):
            for col in sub_pane["data"].columns:
                if col != self.timestamp_col:
                    self.fig.add_trace(go.Scatter(
                        x=sub_pane["data"][self.timestamp_col],
                        y=sub_pane["data"][col],
                        name=f"{sub_pane['name']}_{col}" if sub_pane['name'] else col,
                        mode='lines'
                    ), row=i, col=1)
        
        self.fig.update_layout(
            title=self.title, 
            template='plotly_dark', 
            xaxis_rangeslider_visible=self.rangeslider,
            xaxis=dict(rangebreaks=[dict(bounds=["sat", "mon"])])
        )
        self.fig.show()
        return self
