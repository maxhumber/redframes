from __future__ import annotations

from typing import Any, Callable, Literal, Union

import numpy as np
import pandas as pd
import pandas.core.groupby.generic as pg

Any = Any
Value = Any
Values = list[Value]
OldValue = Value
NewValue = Value
Column = str
Columns = list[Column]
LazyColumns = Union[Column, Columns]
OldColumn = Column
NewColumn = Column
Direction = Literal["up", "down"]
Func = Callable[..., Any]
Join = Literal["left", "right", "inner", "full"]
NumpyArray = np.ndarray
NumpyType = np.dtype
PandasDataFrame = pd.DataFrame
PandasGroupedFrame = pg.DataFrameGroupBy
PandasIndex = pd.Index
PandasRangeIndex = pd.RangeIndex
