import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datasources import DataSource

@dataclass
class DataPlot(ABC):
    datasource: DataSource
    name: str = None
    color: str = 'blue'
    show_on_main: bool = True
    _data: pd.Series = field(default=None, init=False)
    
    def __post_init__(self):
        assert self.datasource is not None, "datasource parameter is required"
    
    @property
    def data(self) -> pd.Series:
        if self._data is None:
            self.calculate()
        return self._data
    
    @data.setter
    def data(self, value: pd.Series):
        self._data = value
    
    @abstractmethod
    def calculate(self) -> pd.Series:
        pass