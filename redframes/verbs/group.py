from __future__ import annotations

from ..types import LazyColumns, PandasDataFrame, PandasGroupedFrame
from ..checks import enforce


def group(df: PandasDataFrame, by: LazyColumns) -> PandasGroupedFrame:
    enforce(by, {list, str})
    gdf = df.groupby(by)
    return gdf
