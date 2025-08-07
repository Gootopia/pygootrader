from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from datetime import datetime
from database import get_client, tick

def get_stock_data(ticker, api_key, days=30):
    """Get OHLC data for a stock ticker using Alpha Vantage"""
    ts = TimeSeries(key=api_key, output_format='pandas')
    data, meta_data = ts.get_daily(symbol=ticker, outputsize='compact')
    
    # Rename columns to match expected format
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    data = data.sort_index()  # Sort by date ascending
    
    return data.tail(days)  # Get last 30 trading days

def display_ohlc(data, ticker):
    """Display OHLC data in a formatted way"""
    print(f"{ticker.upper()} - Last 30 Trading Days")
    print("=" * 60)
    print(f"{'Date':<12} {'Open':<8} {'High':<8} {'Low':<8} {'Close':<8}")
    print("-" * 60)
    
    for date, row in data.iterrows():
        print(f"{date.strftime('%Y-%m-%d'):<12} "
              f"{row['Open']:<8.2f} {row['High']:<8.2f} "
              f"{row['Low']:<8.2f} {row['Close']:<8.2f}")

def ingest_to_influx(data, ticker, client, bucket):
    """Ingest stock data into InfluxDB"""
    for date, row in data.iterrows():
        tick(client, bucket, ticker, row['Open'], row['Close'], row['High'], row['Low'], date.strftime('%Y-%m-%d'))
    print(f"Ingested {len(data)} records for {ticker} into InfluxDB")

if __name__ == "__main__":
    ticker = "SCHB"
    api_key = "YO8PFTVHE5AVIE6NK8"  # Replace with your Alpha Vantage API key
    
    # InfluxDB settings
    url = "http://influxdb:8086"
    token = "mytoken123"
    org = "myorg2"
    bucket = "testing"
    
    if api_key is None:
        print("Please set your Alpha Vantage API key")
        print("Get free API key at: https://www.alphavantage.co/support/#api-key")
    else:
        try:
            data = get_stock_data(ticker, api_key, 30)
            display_ohlc(data, ticker)
            print(f"\nRetrieved {len(data)} trading days of data")
            
            client = get_client(url, token, org)
            ingest_to_influx(data, ticker, client, bucket)
        except Exception as e:
            print(f"Error fetching data: {e}")