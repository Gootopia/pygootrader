from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influx_database import InfluxDatabase
from enum import Enum
from dataclasses import dataclass


class QuoteFields(str, Enum):
    open = "open"
    close = "close"
    high = "high"
    low = "low"
    volume = "volume"
    date = "date"


@dataclass
class TagGroup:
    class Tags(str, Enum):
        Symbol = "symbol"
        DataType = "datatype"
        DataGroup = "datagroup"
    
    symbol: str
    data_type: str
    data_group: str
    
    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}



class TickDatabase(InfluxDatabase):
    def tick(self, ticker: str, open: float, close: float, high: float, low: float, time: str) -> None:
        """Write stock OHLC data to InfluxDB"""
        tags = {"ticker": ticker}
        fields = {QuoteFields.open: open, QuoteFields.close: close, QuoteFields.high: high, QuoteFields.low: low}
        self.write_record(self.dbinfo.bucket, "stock_price", tags, fields)