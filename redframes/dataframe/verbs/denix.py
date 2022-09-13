from __future__ import annotations

from ...types import PandasDataFrame, LazyColumns


def denix(df: PandasDataFrame, columns: LazyColumns | None = None) -> PandasDataFrame:
    is_not_list = not isinstance(columns, list)
    is_not_str = not isinstance(columns, str)
    is_not_none = not columns == None
    if all([is_not_list, is_not_str, is_not_none]):
        raise TypeError("columns type is invalid, must be list[str] | str | None")
    if isinstance(columns, str):
        columns = [columns]
    if isinstance(columns, list):
        bad_keys = set(columns).difference(df.columns)
        if bad_keys:
            if len(bad_keys) == 1:
                message = f"columns argument contains invalid key {bad_keys}"
            else:
                message = f"columns argument contains invalid keys {bad_keys}"
            raise KeyError(message)
    df = df.dropna(subset=columns)
    df = df.reset_index(drop=True)
    return df
