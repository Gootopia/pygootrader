import pandas as pd
import talib
import numpy as np
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod
from tick_database import QuoteFields

class Indicator(ABC):
    @classmethod
    def convert_data(cls, df: pd.DataFrame, column_key: str) -> np.ndarray:
        data = np.array(df[column_key].values, dtype=np.float64)
        return data
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame = None) -> pd.Series:
        pass

@dataclass
class MovingAverage(Indicator):
    class AverageType(Enum):
        Simple = "sma"
        Exponential = "ema"
    
    period: int
    quote_type: QuoteFields = QuoteFields.close
    avg_type: 'MovingAverage.AverageType' = None
    
    def __post_init__(self):
        if self.avg_type is None:
            self.avg_type = self.AverageType.Simple
    
    def calculate(self, df: pd.DataFrame = None) -> pd.Series:
        """Compute moving average for the given data and period"""
        data = self.convert_data(df, self.quote_type.value)
        
        if self.avg_type == self.AverageType.Simple:
            result = pd.Series(talib.SMA(data, timeperiod=self.period))
        elif self.avg_type == self.AverageType.Exponential:
            result = pd.Series(talib.EMA(data, timeperiod=self.period))
        else:
            assert False, f"Invalid avg_type: {self.avg_type}"
        self.data = result
        return result

# class GooEmaDelta(DataPlot): 
#     def __init__(self, 
#                  ema_short: int,
#                  ema_long: int,
#                  period: int,
#                  name: str = None, 
#                  data_type: QuoteFields = QuoteFields.close,
#                  color: str = 'blue', 
#                  show_on_main: bool = True):
#         if name is None:
#             name = f"goo_ema_delta_{ema_short}_{ema_long}_{period}"
#         
#         super().__init__(name, color, show_on_main)
#         
#         self.ema_short = ema_short
#         self.ema_long = ema_long
#         self.period = period
#         self.data_type = data_type
#     
#     def calculate(self) -> pd.Series:
#         """Compute moving average for the given data and period"""
#         data = self.datasource.data[self.data_type].values
#         
#         ema_short = talib.EMA(data, timeperiod=self.ema_short)
#         ema_long = talib.EMA(data, timeperiod=self.ema_long)
# 
#         delta = ema_short - ema_long
#         trend = talib.SMA(delta, timeperiod=self.period)
#         
#         return self.trend