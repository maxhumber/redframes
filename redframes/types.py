from __future__ import annotations

from typing import Any, Callable, Literal, Union

import numpy as np
import pandas as pd
import pandas.core.groupby.generic as pg

Any = Any
Column = str
Columns = list[Column]
Direction = Literal["up", "down"]
Func = Callable[..., Any]
Join = Literal["left", "right", "inner", "full"]
LazyColumns = Union[Column, Columns]
NumpyArray = np.ndarray
PandasDataFrame = pd.DataFrame
PandasGroupedFrame = pg.DataFrameGroupBy
PandasIndex = pd.Index
PandasRangeIndex = pd.RangeIndex
Value = Any
Values = list[Value]