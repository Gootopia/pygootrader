from enum import Enum
from dataclasses import dataclass


class TagCollection:
    def to_dict(self, is_case_sensitive: bool = False):
        result = {k: v.lower() if not is_case_sensitive and isinstance(v, str) else v for k, v in self.__dict__.items() if v is not None}
        return result


@dataclass
class InstrumentTags(TagCollection):
    class Tags(str, Enum):
        Symbol = "symbol"
        DataType = "datatype"
        DataGroup = "datagroup"
    
    symbol: str = None
    data_type: str = None
    data_group: str = None


if __name__ == "__main__":
    tag_group = InstrumentTags(symbol="AAPL", data_type="stock", data_group="equity")
    print(tag_group.to_dict())