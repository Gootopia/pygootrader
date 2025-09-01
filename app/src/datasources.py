from alpha_vantage.timeseries import TimeSeries
from polygon import RESTClient
import pandas as pd
import os
from enum import Enum
from abc import ABC, abstractmethod
from tick_database import QuoteFields
from datetime import datetime, timedelta

class DataSource(ABC):
    @abstractmethod
    def get_data(self, ticker: str = None, time_start=None, time_stop=None) -> pd.DataFrame:
        pass
    
    def get_ticker_details(self, ticker: str = None):
        pass

class Vantage(DataSource):
    def __init__(self, api_key: str = None, output_data_size=None):
        self.api_key = api_key
        self.output_data_size = output_data_size if output_data_size is not None else self.DataOutputSize.Compact
    
    class DataOutputSize(str, Enum):
        Full = 'full'
        Compact = 'compact'
    
    def authenticate(self):
        """Authenticate with Alpha Vantage API"""
        return self.api_key is not None
    
    def get_data(self, ticker: str = None, time_start=None, time_stop=None) -> pd.DataFrame:
        """Get OHLC data for a stock ticker using Alpha Vantage"""
        ts = TimeSeries(key=self.api_key, output_format='pandas')
        data, meta_data = ts.get_daily(symbol=ticker, outputsize=self.output_data_size)
        
        # Rename columns to match expected format
        data.columns = [QuoteFields.open.title(), 
                       QuoteFields.high.title(), 
                       QuoteFields.low.title(), 
                       QuoteFields.close.title(), 
                       QuoteFields.volume.title()]
        data = data.sort_index()  # Sort by date ascending
        
        return data.tail(days)

class PolygonIO(DataSource):
    # Polygon IO docs:
    # https://polygon-api-client.readthedocs.io/en/latest/
    # NOTE: Polygon calls use POLYGON_API_KEY environment variable by default
    def __init__(self, api_key: str = None):
        # API key injected below for easy use. If not provided, the script will attempt
        # to use the environment variable "POLYGON_API_KEY".
        if api_key is None:
            api_key = DataSourceHelpers.get_api_key("POLYGON_API_KEY")
        
        client = RESTClient(api_key)
        self.client = client
    
    def get_data(self, ticker: str = None, time_start=None, time_stop=None) -> pd.DataFrame:
        """Get OHLC data for a stock ticker using Polygon.io"""
        if time_stop is None:
            time_stop = datetime.now().strftime("%Y-%m-%d")
        if time_start is None:
            time_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        aggs = self.client.get_aggs(ticker, 1, "day", time_start, time_stop)
        aggs_df = self._convert_to_pandas_dataframe(aggs)
        return aggs_df
    
    def get_ticker_details(self, ticker: str = None):
        """Get ticker details using Polygon.io"""
        return self.client.get_ticker_details(ticker)
    
    def _convert_to_pandas_dataframe(self, data: list) -> pd.DataFrame:
        """Convert Polygon.io data to pandas DataFrame"""
        df_data = []
        for item in data:
            df_data.append({
                QuoteFields.open.value: item.open,
                QuoteFields.close.value: item.close,
                QuoteFields.high.value: item.high,
                QuoteFields.low.value: item.low,
                QuoteFields.date.value: item.timestamp
            })
        df = pd.DataFrame(df_data)
        return df

class DataSourceHelpers:
    @classmethod
    def get_api_key(cls, api_key_name: str = None):
        # setx <env_name_in_caps> "<your_api_key>"   <- windows
        # export <env_name_in_caps> ="<your_api_key>" <- mac/linux
        assert api_key_name is not None, "api_key_name parameter is required"
        assert api_key_name in os.environ, f"Environment variable '{api_key_name}' not found"
        return os.environ[api_key_name]
    
    @classmethod
    def display_ohlc(cls, data: pd.DataFrame, ticker, convert_utc: bool = True):
        """Display OHLC data in a formatted way"""
        print(f"{ticker.upper()}")
        print("=" * 60)
        print(f"{QuoteFields.date.value:<12} "
              f"{QuoteFields.open.value:<8} "
              f"{QuoteFields.high.value:<8} "
              f"{QuoteFields.low.value:<8} "
              f"{QuoteFields.close.value:<8}")
        print("-" * 60)
        
        for index, row in data.iterrows():
            date_value = row[QuoteFields.date.value]
            if convert_utc:
                date_value = datetime.fromtimestamp(date_value / 1000).strftime("%Y-%m-%d")
            print(f"{date_value:<12} "
                  f"{row[QuoteFields.open.value]:<8.2f} "
                  f"{row[QuoteFields.high.value]:<8.2f} "
                  f"{row[QuoteFields.low.value]:<8.2f} "
                  f"{row[QuoteFields.close.value]:<8.2f}")

if __name__ == "__main__":
    p = PolygonIO()
    r = p.get_ticker_details("SPY")
    data = p.get_data("SPY", "2010-01-01")
    DataSourceHelpers.display_ohlc(data, "SPY")
    print("DataSources module loaded")