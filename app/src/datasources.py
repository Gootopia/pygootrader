# Built-in packages
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Union
import pytz

# Third-party packages
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from polygon import RESTClient
from loguru import logger

# Custom packages
from tick_database import QuoteFields
from influx_database import InfluxDatabase

class DataSource(ABC):
    def __init__(self) -> None:
        self._data = None
    
    @property
    def data(self) -> pd.DataFrame:
        return self._data
    
    @data.setter
    def data(self, value: pd.DataFrame) -> None:
        self._data = value
    
    @abstractmethod
    def download_data(self, ticker: Optional[str] = None, 
                     time_start: Optional[str] = None, 
                     time_stop: Optional[str] = None) -> pd.DataFrame:
        pass
    
    def get_ticker_details(self, ticker: Optional[str] = None) -> Any:
        pass

class Vantage(DataSource):
    def __init__(self, api_key: Optional[str] = None, 
                 output_data_size: Optional['Vantage.DataOutputSize'] = None) -> None:
        super().__init__()
        self.api_key = api_key
        self.output_data_size = output_data_size if output_data_size is not None else self.DataOutputSize.Compact
    
    class DataOutputSize(str, Enum):
        Full = 'full'
        Compact = 'compact'
    
    def authenticate(self) -> bool:
        """Authenticate with Alpha Vantage API"""
        return self.api_key is not None
    
    def download_data(self, ticker: Optional[str] = None, 
                     time_start: Optional[str] = None, 
                     time_stop: Optional[str] = None) -> pd.DataFrame:
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
        
        return data

class PolygonIO(DataSource):
    # Polygon IO docs:
    # https://polygon-api-client.readthedocs.io/en/latest/
    # NOTE: Polygon calls use POLYGON_API_KEY environment variable by default
    
    # timestamp column name for data downloaded from PolygonIo
    # needed for ingesting data into InfluxDb
    POLYGON_TIMESTAMP_KEY = "_time"
    
    def __init__(self, api_key: Optional[str] = None) -> None:
        super().__init__()
        # API key injected below for easy use. If not provided, the script will attempt
        # to use the environment variable "POLYGON_API_KEY".
        if api_key is None:
            api_key = DataSourceHelpers.get_api_key("POLYGON_API_KEY")
        
        client = RESTClient(api_key)
        self.client = client
    
    def download_data(self, ticker: Optional[str] = None, 
                     time_start: Optional[str] = None, 
                     time_stop: Optional[str] = None) -> pd.DataFrame:
        """Get OHLC data for a stock ticker using Polygon.io"""
        assert ticker is not None, "ticker parameter is required"
        ticker = ticker.upper()

        if time_stop is None:
            time_stop = datetime.now().strftime("%Y-%m-%d")
        if time_start is None:
            time_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        tick_data = self.client.get_aggs(ticker, 1, "day", time_start, time_stop)
        tick_data_df = self._convert_to_pandas_dataframe(tick_data)
        self.data = tick_data_df
        
        return tick_data_df
    
    def get_ticker_details(self, ticker: Optional[str] = None) -> Any:
        """Get ticker details using Polygon.io"""
        ticker = ticker.upper() if ticker else ticker
        return self.client.get_ticker_details(ticker)
    
    def _convert_to_pandas_dataframe(self, data: List[Any], 
                                    time_column_name: str = QuoteFields.time_influx.value) -> pd.DataFrame:
        """Convert Polygon.io data to pandas DataFrame with EST timezone conversion"""
        df_data = []
        est_tz = pytz.timezone('US/Eastern')
        
        for item in data:
            # Convert UTC timestamp to EST
            utc_dt = datetime.fromtimestamp(item.timestamp / 1000, tz=timezone.utc)
            est_dt = utc_dt.astimezone(est_tz)
            
            df_data.append({
                QuoteFields.open.value: item.open,
                QuoteFields.close.value: item.close,
                QuoteFields.high.value: item.high,
                QuoteFields.low.value: item.low,
                time_column_name: est_dt.strftime("%m/%d/%Y")
            })
        df = pd.DataFrame(df_data)
        return df

class DataSourceHelpers:
    @classmethod
    def get_api_key(cls, api_key_name: Optional[str] = None) -> str:
        # setx <env_name_in_caps> "<your_api_key>"   <- windows
        # export <env_name_in_caps> ="<your_api_key>" <- mac/linux
        assert api_key_name is not None, "api_key_name parameter is required"
        assert api_key_name in os.environ, f"Environment variable '{api_key_name}' not found"
        return os.environ[api_key_name]
    
    @classmethod
    def bulk_update_data(cls, datasource: Optional[DataSource] = None, 
                        influx_db: Optional[InfluxDatabase] = None, 
                        symbols: Optional[Union[str, List[str]]] = None,
                        ingest_to_db: bool = True) -> None:
        from tags import InstrumentTags
        
        assert datasource is not None, "datasource parameter is required"
        assert symbols is not None, "symbols parameter is required"
        
        if ingest_to_db:
            assert influx_db is not None, "influx_db parameter is required when ingest_to_db is True"
            assert influx_db.get_connection_status(), "InfluxDB is not connected"
        
        # Convert single string to list
        if isinstance(symbols, str):
            symbols = [symbols]
        
        for symbol in symbols:
            data = datasource.download_data(symbol)
            if ingest_to_db:
                tags = InstrumentTags(symbol=symbol.lower())
                logger.info(f"Writing {symbol} to database with {len(data)} records")
                influx_db.write_pandas(dataframe=data, tags=tags, timestamp_key="_time")
    
    @classmethod
    def display_ohlc(cls, data: pd.DataFrame, 
                    ticker: str, 
                    convert_utc: bool = False) -> None:
        """Display OHLC data in a formatted way"""
        # Find time field that exists in dataframe columns
        time_field = None
        for field in QuoteFields:
            if field.name.startswith("time_") and field.value in data.columns:
                time_field = field.value
                break
        
        assert time_field is not None, "No valid time field found in dataframe columns"
        
        print(f"{ticker.upper()}")
        print("=" * 60)
        print(f"{time_field:<12} "
              f"{QuoteFields.open.value:<8} "
              f"{QuoteFields.high.value:<8} "
              f"{QuoteFields.low.value:<8} "
              f"{QuoteFields.close.value:<8}")
        print("-" * 60)
        
        for index, row in data.iterrows():
            date_value = row[time_field]
            if convert_utc:
                date_value = datetime.fromtimestamp(date_value / 1000).strftime("%Y-%m-%d")
            print(f"{date_value:<12} "
                  f"{row[QuoteFields.open.value]:<8.2f} "
                  f"{row[QuoteFields.high.value]:<8.2f} "
                  f"{row[QuoteFields.low.value]:<8.2f} "
                  f"{row[QuoteFields.close.value]:<8.2f}")

if __name__ == "__main__":
    p = PolygonIO()
    db = InfluxDatabase()
    symbols = db.get_tag_values("tick_data", "symbol")
    r = p.get_ticker_details("schb")
    print(r)
    symbols = ['schb', 'gdx', 'schz']
    DataSourceHelpers.bulk_update_data(p, db, symbols)

    print("DataSources module loaded")