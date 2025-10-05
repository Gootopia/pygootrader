from datetime import datetime
import plotly.graph_objects as go
from tick_database import QuoteFields
from datasources import PolygonIO, DataSourceHelpers
from technical_analysis import MovingAverage
from influx_database import InfluxDatabase
from tags import InstrumentTags

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
    fig.add_trace(go.Candlestick(x=formatted_dates,
                          open=data[QuoteFields.open],
                          high=data[QuoteFields.high],
                          low=data[QuoteFields.low],
                          close=data[QuoteFields.close]))
    
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


if __name__ == "__main__":   
    ds_polygon = PolygonIO()
    try:
        ticker = "SPY"
        data = ds_polygon.download_data(ticker, "2022-01-01")
        DataSourceHelpers.display_ohlc(data,ticker)
        
        db = InfluxDatabase()
        tags = InstrumentTags(symbol=ticker)
        db.write_pandas(dataframe=data,tags=tags)

        #ema_5 = MovingAverage(ds_polygon, 5)
        #ema_50 = MovingAverage(ds_polygon, 50, color='green')
        ema_200 = MovingAverage(ds_polygon, 200, color='red')
        #ema_5.calculate()
        #ema_50.calculate()
        ema_200.calculate()
        sub_plots = [ema_200]
        plot_ohlc(data, ticker, sub_plots)

    except Exception as e:
        print(f"Error fetching data: {e}")