import pandas as pd
import talib
from dataclasses import dataclass
from enum import Enum
from tick_database import QuoteFields
from datasources import DataSource
from charts import DataPlot

class MovingAverage(DataPlot):
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

class GooEmaDelta(DataPlot): 
    def __init__(self, 
                 datasource: DataSource,
                 ema_short: int,
                 ema_long: int,
                 period: int,
                 name: str = None, 
                 data_type: QuoteFields = QuoteFields.close,
                 color: str = 'blue', 
                 show_on_main: bool = True):
        if name is None:
            name = f"goo_ema_delta_{ema_short}_{ema_long}_{period}"
        
        super().__init__(datasource, name, color, show_on_main)
        
        self.ema_short = ema_short
        self.ema_long = ema_long
        self.period = period
        self.data_type = data_type
    
    def calculate(self) -> pd.Series:
        """Compute moving average for the given data and period"""
        data = self.datasource.data[self.data_type].values
        
        ema_short = talib.EMA(data, timeperiod=self.ema_short)
        ema_long = talib.EMA(data, timeperiod=self.ema_long)

        delta = ema_short - ema_long
        trend = talib.SMA(delta, timeperiod=self.period)
        
        return self.trend