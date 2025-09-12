import pandas as pd
import talib
from datetime import datetime
import plotly.graph_objects as go
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from tick_database import QuoteFields
from datasources import PolygonIO, DataSourceHelpers, DataSource
#from database import get_client, tick

@dataclass
class Subplot(ABC):
    datasource: DataSource
    name: str
    color: str = 'blue'
    show_on_main: bool = True
    _data: pd.Series = field(default=None, init=False)
    
    def __post_init__(self):
        assert self.datasource is not None, "datasource parameter is required"
    
    @property
    def data(self) -> pd.Series:
        if self._data is None:
            self.calculate()
        return self._data
    
    @data.setter
    def data(self, value: pd.Series):
        self._data = value
    
    @abstractmethod
    def calculate(self) -> pd.Series:
        pass

class MovingAverage(Subplot):
    class AverageType(Enum):
        Simple = "sma"
        Exponential = "ema"
        
    def __init__(self, 
                 datasource: DataSource,
                 period: int,
                 name: str = None, 
                 data_type: QuoteFields = QuoteFields.close,
                 color: str = 'blue', 
                 show_on_main: bool = True,
                 avg_type: AverageType = AverageType.Simple):
        if name is None:
            name = f"{avg_type.value}{period}"
        
        super().__init__(datasource, name, color, show_on_main)
        
        self.period = period
        self.data_type = data_type
        self.avg_type = avg_type
    
    def calculate(self) -> pd.Series:
        """Compute moving average for the given data and period"""
        data = self.datasource.data[self.data_type].values
        
        if self.avg_type == self.AverageType.Simple:
            self.data = pd.Series(talib.SMA(data, timeperiod=self.period))
        elif self.avg_type == self.AverageType.Exponential:
            self.data = pd.Series(talib.EMA(data, timeperiod=self.period))
        else:
            assert False, f"Invalid avg_type: {self.avg_type}"
        
        return self.data

def plot_ohlc(data, ticker, subplots=None):
    """Display OHLC data as a candlestick chart with optional subplot data"""
    from plotly.subplots import make_subplots
    
    # Format dates to YYYY-MM-DD
    formatted_dates = [datetime.fromtimestamp(row[QuoteFields.date.value] / 1000).strftime('%Y-%m-%d') 
                      for _, row in data.iterrows()]
    
    # Count subplots needed
    subplot_count = 1
    if subplots:
        subplot_count += sum(1 for subplot in subplots if not subplot.show_on_main)
    
    # Create subplots
    fig = make_subplots(rows=subplot_count, cols=1, shared_xaxes=True)
    
    # Add main OHLC data
    fig.add_trace(go.Ohlc(x=formatted_dates,
                          open=data[QuoteFields.open],
                          high=data[QuoteFields.high],
                          low=data[QuoteFields.low],
                          close=data[QuoteFields.close],
                          hovertext=[f'Date: {date}<br>Open: ${row[QuoteFields.open]:.2f}<br>' +
                                     f'High: ${row[QuoteFields.high]:.2f}<br>' +
                                     f'Low: ${row[QuoteFields.low]:.2f}<br>' +
                                     f'Close: ${row[QuoteFields.close]:.2f}'
                                     for date, (_, row) in zip(formatted_dates, data.iterrows())]), row=1, col=1)
    
    # Add subplot data
    if subplots:
        subplot_row = 2
        for subplot in subplots:
            if subplot.show_on_main:
                # Add to main plot
                fig.add_trace(go.Scatter(x=formatted_dates, y=subplot.data, name=subplot.name, 
                                         mode='lines', line=dict(color=subplot.color)), 
                              row=1, col=1)
            else:
                # Add to individual subplot
                fig.add_trace(go.Scatter(x=formatted_dates, y=subplot.data, name=subplot.name, 
                                         mode='lines', line=dict(color=subplot.color)), 
                              row=subplot_row, col=1)
                subplot_row += 1
    
    fig.update_layout(title=f'{ticker.upper()} - OHLC Bar Chart',
                      template='plotly_dark',
                      plot_bgcolor='black',
                      paper_bgcolor='black',
                      xaxis=dict(
                          type='category',
                          categoryorder='category ascending',
                          rangeslider=dict(visible=False),
                          tickvals=[date for i, date in enumerate(formatted_dates) 
                                   if datetime.fromtimestamp(data.iloc[i][QuoteFields.date.value] / 1000).weekday() == 0 and 
                                      datetime.fromtimestamp(data.iloc[i][QuoteFields.date.value] / 1000).day <= 7],
                          ticktext=[date for i, date in enumerate(formatted_dates) 
                                   if datetime.fromtimestamp(data.iloc[i][QuoteFields.date.value] / 1000).weekday() == 0 and 
                                      datetime.fromtimestamp(data.iloc[i][QuoteFields.date.value] / 1000).day <= 7]
                      ))
    fig.show()


def ingest_to_influx(data, ticker, client, bucket):
    """Ingest stock data into InfluxDB"""
    for date, row in data.iterrows():
        tick(client, bucket, ticker, row['Open'], row['Close'], row['High'], row['Low'], date.strftime('%Y-%m-%d'))
    print(f"Ingested {len(data)} records for {ticker} into InfluxDB")

if __name__ == "__main__":   
    # InfluxDB settings
    url = "http://influxdb:8086"
    token = "mytoken123"
    org = "myorg2"
    bucket = "testing"
    
    ds_polygon = PolygonIO()
    try:
        ticker = "SPY"
        data = ds_polygon.download_data(ticker, "2022-01-01")
        DataSourceHelpers.display_ohlc(data,ticker)

        ema_5 = MovingAverage(ds_polygon, 5)
        ema_50 = MovingAverage(ds_polygon, 50, color='green')
        ema_200 = MovingAverage(ds_polygon, 200, color='red')
        ema_5.calculate()
        ema_50.calculate()
        ema_200.calculate()
        sub_plots = [ema_5,ema_50,ema_200]
        plot_ohlc(data, ticker, sub_plots)
        
        client = get_client(url, token, org)
        ingest_to_influx(data, ticker, client, bucket)
    except Exception as e:
        print(f"Error fetching data: {e}")