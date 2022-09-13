from ..types import Func, PandasDataFrame


def filter(df: PandasDataFrame, func: Func) -> PandasDataFrame:
    if not callable(func):
        raise TypeError("func type is invalid, must be Callable[..., bool]")
    df = df.loc[func]  # type: ignore
    df = df.reset_index(drop=True)
    return df
