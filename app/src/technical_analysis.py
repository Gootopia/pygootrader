# Built-in packages
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import wraps

# Third-party packages
import numpy as np
import pandas as pd
import talib

# Custom packages
from charts import Colormap
from tick_database import QuoteFields

def validate_dataframe(func):
    @wraps(func)
    def wrapper(self, df=None, *args, **kwargs):
        assert df is not None, "DataFrame parameter cannot be None"
        assert not df.empty, "DataFrame parameter cannot be empty"
        return func(self, df, *args, **kwargs)
    return wrapper

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
    
    @validate_dataframe
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

@dataclass
class GooEmaDelta(Indicator, Colormap):
    ema_short: int
    ema_long: int
    period: int
    quote_type: QuoteFields = QuoteFields.close
    
    @validate_dataframe
    def calculate(self, df: pd.DataFrame = None) -> pd.Series:
        """Compute EMA delta trend for the given data and parameters"""
        data = self.convert_data(df, self.quote_type.value)
        
        ema_short = talib.EMA(data, timeperiod=self.ema_short)
        ema_long = talib.EMA(data, timeperiod=self.ema_long)
        
        delta = ema_short - ema_long
        result = pd.Series(talib.SMA(delta, timeperiod=self.period))
        
        return result
    
    def map_value_to_color(self, values: list, dataframe: pd.DataFrame = None) -> list:
        colors = []
        for i, current_value in enumerate(values):
            if i == 0:
                colors.append('gray')
            elif current_value > values[i-1]:
                colors.append('green')
            elif current_value < values[i-1]:
                colors.append('red')
            else:
                colors.append('gray')
        return colors