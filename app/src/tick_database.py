# Built-in packages
from enum import Enum

# Third-party packages
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Custom packages
from influx_database import InfluxDatabase

class QuoteFields(str, Enum):
    open = "open"
    close = "close"
    high = "high"
    low = "low"
    volume = "volume"
    time = "_time"  # underscore is required due to InfluxDB


class TickDatabase(InfluxDatabase):
    def tick(self, ticker: str, open: float, close: float, high: float, low: float, time: str) -> None:
        """Write stock OHLC data to InfluxDB"""
        tags = {"ticker": ticker}
        fields = {QuoteFields.open: open, QuoteFields.close: close, QuoteFields.high: high, QuoteFields.low: low}
        self.write_record(self.dbinfo.bucket, "stock_price", tags, fields)