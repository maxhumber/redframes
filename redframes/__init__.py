from __future__ import annotations
from typing import Any
import pandas as pd

from .init import init
from .take import take

class DataFrame:
    def __init__(self, data: pd.DataFrame | dict[str, list[Any]] | str = pd.DataFrame()):
        """
        >>> df = DataFrame({"foo": [1, 2, 3]})
        >>> df
           foo
        0    1
        1    2
        2    3
        """
        self._data = init(data=data)