from __future__ import annotations

from ...types import LazyColumns, PandasDataFrame, PandasGroupedFrame

from ._validate import _validate_columns_type_list_str


def group(df: PandasDataFrame, by: LazyColumns) -> PandasGroupedFrame:
    _validate_columns_type_list_str(by)
    gdf = df.groupby(by)
    return gdf
