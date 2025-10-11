import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tick_database import QuoteFields
from enum import Enum
from dataclasses import dataclass
from typing import Union
from tags import InstrumentTags

@dataclass
class PlotAttribute:
    color: str = 'blue'
    linewidth: int = 1

class Chart:
    class ChartType(Enum):
        OHLC = "ohlc"
        CANDLESTICK = "candlestick"
    
    def __init__(self, 
                 symbol: Union[str, InstrumentTags] = "Chart", 
                 rangeslider: bool = False, 
                 chart_type: ChartType = None):
        if isinstance(symbol, InstrumentTags):
            self.symbol = symbol.symbol.upper() if symbol.symbol else "Chart"
        else:
            self.symbol = symbol
        self.rangeslider = rangeslider
        self.main_data = None
        self.timestamp_col = None
        self.chart_type = chart_type if chart_type is not None else self.ChartType.OHLC
        self.sub_panes = []
        self.fig = None
    
    def data(self, 
             data: pd.DataFrame, 
             timestamp_col: str = QuoteFields.time.value):
        self.main_data = data
        self.timestamp_col = timestamp_col
        return self
    
    def chart_type(self, chart_type: ChartType):
        self.chart_type = chart_type
        return self
    
    def add_sub_plot(self, 
                     data: pd.Series, 
                     pane_index: int = 0, 
                     name: str = None, 
                     plot_attribute: PlotAttribute = None):
        if name is None:
            name = data.name if data.name is not None else "Series"
        self.sub_panes.append({"data": data, "name": name, "pane_index": pane_index, "plot_attribute": plot_attribute})
        return self
    
    def show(self):
        # Dictionary key constants
        DATA_KEY = "data"
        NAME_KEY = "name"
        PANE_INDEX_KEY = "pane_index"
        PLOT_ATTRIBUTE_KEY = "plot_attribute"
        max_pane_index = max([sp[PANE_INDEX_KEY] for sp in self.sub_panes], default=0)
        subplot_count = max(1, max_pane_index + 1)
        self.fig = make_subplots(rows=subplot_count, cols=1, shared_xaxes=True)
        
        # Add main chart
        if self.chart_type == self.ChartType.CANDLESTICK:
            self.fig.add_trace(go.Candlestick(
                x=self.main_data[self.timestamp_col],
                open=self.main_data[QuoteFields.open.value],
                high=self.main_data[QuoteFields.high.value],
                low=self.main_data[QuoteFields.low.value],
                close=self.main_data[QuoteFields.close.value],
                name=self.symbol
            ), row=1, col=1)
        else:  # OHLC
            self.fig.add_trace(go.Ohlc(
                x=self.main_data[self.timestamp_col],
                open=self.main_data[QuoteFields.open.value],
                high=self.main_data[QuoteFields.high.value],
                low=self.main_data[QuoteFields.low.value],
                close=self.main_data[QuoteFields.close.value],
                name=self.symbol
            ), row=1, col=1)
        
        # Add sub-plots
        for sub_pane in self.sub_panes:
            row_index = 1 if sub_pane[PANE_INDEX_KEY] == 0 else sub_pane[PANE_INDEX_KEY] + 1
            line_dict = {}
            if sub_pane[PLOT_ATTRIBUTE_KEY] is not None:
                if sub_pane[PLOT_ATTRIBUTE_KEY].color is not None:
                    line_dict['color'] = sub_pane[PLOT_ATTRIBUTE_KEY].color
                if sub_pane[PLOT_ATTRIBUTE_KEY].linewidth is not None:
                    line_dict['width'] = sub_pane[PLOT_ATTRIBUTE_KEY].linewidth
            
            self.fig.add_trace(go.Scatter(
                x=self.main_data[self.timestamp_col],
                y=sub_pane[DATA_KEY],
                name=sub_pane[NAME_KEY] if sub_pane[NAME_KEY] else "Series",
                mode='lines',
                line=line_dict if line_dict else None
            ), row=row_index, col=1)
        
        self.fig.update_layout(
            template='plotly_dark', 
            xaxis_rangeslider_visible=self.rangeslider,
            xaxis=dict(rangebreaks=[dict(bounds=[6, 1], pattern="day of week")], uirevision="constant"),
            yaxis=dict(uirevision="constant"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(l=20, r=20, t=40, b=20),
            uirevision="constant",
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    buttons=list([
                        dict(
                            args=[{"type": "candlestick"}, [0]],
                            label="Candlestick",
                            method="restyle"
                        ),
                        dict(
                            args=[{"type": "ohlc"}, [0]],
                            label="OHLC",
                            method="restyle"
                        )
                    ]),
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.01,
                    xanchor="left",
                    y=0.98,
                    yanchor="top"
                ),
            ]
        )
        self.fig.show()
        return self
