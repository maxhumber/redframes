from __future__ import annotations

from ..checks import _check_type
from ..types import Column, PandasDataFrame, PandasGroupedFrame


def pack(
    df: PandasDataFrame | PandasGroupedFrame, column: Column, sep: str
) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(sep, str)
    order = df.obj.columns if isinstance(df, PandasGroupedFrame) else df.columns
    df = df.agg(**{column: (column, lambda x: x.str.cat(sep=sep))})
    df = df[order]
    df = df.reset_index(drop=True)
    return df

# def pack(
#     df: PandasDataFrame | PandasGroupedFrame, column: Column, sep: str
# ) -> PandasDataFrame:
#     _check_type(column, str)
#     _check_type(sep, str)
#     if isinstance(df, PandasGroupedFrame):
#         order = df.obj.columns
#         print(order)
#         # drop = False
#     # else: 
#     #     order = df.columns
#     #     drop = True
#     drop = isinstance(df, PandasDataFrame)
#     df = df.agg(**{column: (column, lambda x: x.str.cat(sep=sep))})
#     # df = df[order]
#     # df = df.reset_index(drop=drop)
#     df = df.reset_index(drop=True)
#     return df
