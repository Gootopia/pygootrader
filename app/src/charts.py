# Built-in packages
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Union

# Third-party packages
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Custom packages
from tags import InstrumentTags
from tick_database import QuoteFields

class Colormap(ABC):
    @abstractmethod
    def map_value_to_color(self, values: list, dataframe: pd.DataFrame = None) -> list:
        pass

@dataclass
class PlotAttribute:
    class LineStyle(Enum):
        Line = "line"
        Histogram = "histogram"
    
    color: str = 'blue'
    linewidth: int = 1
    line_style: 'PlotAttribute.LineStyle' = LineStyle.Line

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
             timestamp_col: str = QuoteFields.time_influx.value):
        self.main_data = data
        self.timestamp_col = timestamp_col
        return self
    
    def chart_type(self, chart_type: ChartType):
        self.chart_type = chart_type
        return self
    
    def add_sub_plot(self, 
                     data, 
                     pane_index: int = 0, 
                     name: str = None, 
                     plot_attribute: PlotAttribute = None):
        # Call calculate method if it exists
        if hasattr(data, 'calculate'):
            calculated_data = data.calculate(self.main_data)
        else:
            assert False, "Data object must have a calculate method"
            
        if name is None:
            name = calculated_data.name if hasattr(calculated_data, 'name') and calculated_data.name is not None else "Series"
        
        # Check if data object is derived from Colormap and add color_map
        color_map = None
        if isinstance(data, Colormap):
            values = calculated_data.tolist()
            color_map = data.map_value_to_color(values, self.main_data)
            
        self.sub_panes.append({"data": calculated_data, "name": name, "pane_index": pane_index, "plot_attribute": plot_attribute, "data_object": data, "color_map": color_map})
        return self
    
    def show(self, scale_sub_pane: float = 0.33):
        # Dictionary key constants
        DATA_KEY = "data"
        NAME_KEY = "name"
        PANE_INDEX_KEY = "pane_index"
        PLOT_ATTRIBUTE_KEY = "plot_attribute"
        COLOR_MAP_KEY = "color_map"
        max_pane_index = max([sp[PANE_INDEX_KEY] for sp in self.sub_panes], default=0)
        subplot_count = max(1, max_pane_index + 1)
        
        # Calculate row heights - main pane gets 1.0, sub-panes get scale_sub_pane
        row_heights = [1.0] + [scale_sub_pane] * (subplot_count - 1)
        
        # Create subplot titles
        subplot_titles = [self.symbol] + [sp[NAME_KEY] for sp in self.sub_panes if sp[PANE_INDEX_KEY] > 0]
        
        self.fig = make_subplots(rows=subplot_count, cols=1, shared_xaxes=True, 
                                vertical_spacing=0.02, row_heights=row_heights,
                                subplot_titles=subplot_titles)
        
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
        for sub_pane_dict in self.sub_panes:
            row_index = 1 if sub_pane_dict[PANE_INDEX_KEY] == 0 else sub_pane_dict[PANE_INDEX_KEY] + 1
            
            # Check if this should be a histogram
            is_histogram = (sub_pane_dict[PLOT_ATTRIBUTE_KEY] is not None and 
                           sub_pane_dict[PLOT_ATTRIBUTE_KEY].line_style == PlotAttribute.LineStyle.Histogram)
            
            # Generate colors using colormap if provided
            colors = sub_pane_dict[COLOR_MAP_KEY] if sub_pane_dict[COLOR_MAP_KEY] is not None else None
            
            if is_histogram:
                marker_dict = {}
                if colors is not None:
                    marker_dict['color'] = colors
                elif sub_pane_dict[PLOT_ATTRIBUTE_KEY] and sub_pane_dict[PLOT_ATTRIBUTE_KEY].color is not None:
                    marker_dict['color'] = sub_pane_dict[PLOT_ATTRIBUTE_KEY].color
                
                self.fig.add_trace(go.Bar(
                    x=self.main_data[self.timestamp_col],
                    y=sub_pane_dict[DATA_KEY],
                    name=sub_pane_dict[NAME_KEY] if sub_pane_dict[NAME_KEY] else "Series",
                    marker=marker_dict if marker_dict else None
                ), row=row_index, col=1)
            else:
                line_dict = {}
                marker_dict = {}
                
                if colors is not None:
                    marker_dict['color'] = colors
                elif sub_pane_dict[PLOT_ATTRIBUTE_KEY] is not None:
                    if sub_pane_dict[PLOT_ATTRIBUTE_KEY].color is not None:
                        line_dict['color'] = sub_pane_dict[PLOT_ATTRIBUTE_KEY].color
                    if sub_pane_dict[PLOT_ATTRIBUTE_KEY].linewidth is not None:
                        line_dict['width'] = sub_pane_dict[PLOT_ATTRIBUTE_KEY].linewidth
                
                self.fig.add_trace(go.Scatter(
                    x=self.main_data[self.timestamp_col],
                    y=sub_pane_dict[DATA_KEY],
                    name=sub_pane_dict[NAME_KEY] if sub_pane_dict[NAME_KEY] else "Series",
                    mode='lines' if not colors else 'markers',
                    line=line_dict if line_dict else None,
                    marker=marker_dict if marker_dict else None
                ), row=row_index, col=1)
        
        # Apply rangebreaks to all xaxis subplots
        for i in range(1, subplot_count + 1):
            xaxis_key = 'xaxis' if i == 1 else f'xaxis{i}'
            yaxis_key = 'yaxis' if i == 1 else f'yaxis{i}'
            self.fig.update_layout(**{
                xaxis_key: dict(rangebreaks=[dict(bounds=[6, 1], pattern="day of week")], uirevision="constant", showgrid=True),
                yaxis_key: dict(uirevision="constant")
            })
        
        self.fig.update_layout(
            template='plotly_dark', 
            xaxis_rangeslider_visible=self.rangeslider,
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
