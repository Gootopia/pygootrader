from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influx_database import InfluxDatabase


class TickDatabase(InfluxDatabase):
    def tick(self, ticker: str, open: float, close: float, high: float, low: float, time: str) -> None:
        """Write stock OHLC data to InfluxDB"""
        tags = {"ticker": ticker}
        fields = {"open": open, "close": close, "high": high, "low": low}
        self.write_record(self.dbinfo.bucket, "stock_price", tags, fields)