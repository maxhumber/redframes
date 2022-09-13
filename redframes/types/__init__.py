from __future__ import annotations

from typing import Any, Callable, Literal, Union

import pandas as pd
import pandas.core.groupby.generic as pg

Any = Any
PandasDataFrame = pd.DataFrame
PandasGroupedDataFrame = pg.DataFrameGroupBy
PandasCommonFrame = Union[PandasDataFrame, PandasGroupedDataFrame]
Column = str
Columns = list[Column]
ColumnsU = Union[Column, Columns]
New = str
Old = str
Value = Any
Values = list[Value]
Direction = Literal["up", "down"]
Join = Literal["left", "right", "inner", "full"]
Rank = Literal["dense", "first", "min"]
Func = Callable[..., Any]
Dimensions = dict[str, int]
Rows = int
Percent = float
