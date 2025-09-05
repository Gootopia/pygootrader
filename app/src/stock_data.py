import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
from tick_database import QuoteFields
from datasources import PolygonIO, DataSourceHelpers
#from database import get_client, tick

class Subplot:
    def __init__(self, name: str, data: pd.Series, color: str = 'blue'):
        self.name = name
        self.data = data
        self.color = color

class Ema(Subplot):
    def __init__(self, name: str, data: pd.Series, period: int, data_type: QuoteFields = QuoteFields.close):
        if name is None:
            name = f"{period}Ema"
        
        super().__init__(name, data)
        
        self.period = period
        self.data_type = data_type
    
    def calculate(self, data: pd.DataFrame, period: int, type: QuoteFields = QuoteFields.close) -> pd.Series:
        """Compute exponential moving average for the given data and period"""
        self.data = data[type].ewm(span=period).mean()
        return self.data

def plot_ohlc(data, ticker, additional_data=None):
    """Display OHLC data as a candlestick chart with optional additional data"""
    from plotly.subplots import make_subplots
    
    # Format dates to YYYY-MM-DD
    formatted_dates = [datetime.fromtimestamp(row[QuoteFields.date.value] / 1000).strftime('%Y-%m-%d') 
                      for _, row in data.iterrows()]
    
    # Count subplots needed
    subplot_count = 1
    if additional_data:
        subplot_count += sum(1 for item in additional_data.values() if not item.get('main_pane', False))
    
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
    
    # Add additional data
    if additional_data:
        subplot_row = 2
        for name, item in additional_data.items():
            subplot_obj = item['data']
            is_main = item.get('main_pane', False)
            
            if is_main:
                # Add to main plot
                fig.add_trace(go.Scatter(x=formatted_dates, y=subplot_obj.data, name=name, 
                                         mode='lines', line=dict(color=subplot_obj.color)), 
                              row=1, col=1)
            else:
                # Add to subplot
                fig.add_trace(go.Scatter(x=formatted_dates, y=subplot_obj.data, name=name, 
                                         mode='lines', line=dict(color=subplot_obj.color)), 
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
        data = ds_polygon.get_data(ticker, "2022-01-01")
        DataSourceHelpers.display_ohlc(data,ticker)

        ema = Ema(None, None, 20)
        ema_data = ema.calculate(data, 20)
        additional_data = {"20EMA": {"data": ema, "main_pane": True}}
        plot_ohlc(data, ticker, additional_data)
        
        client = get_client(url, token, org)
        ingest_to_influx(data, ticker, client, bucket)
    except Exception as e:
        print(f"Error fetching data: {e}")