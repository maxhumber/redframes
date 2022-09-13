import pandas as pd

from ..types import PandasDataFrame


def append(top: PandasDataFrame, bottom: PandasDataFrame) -> PandasDataFrame:
    df = pd.concat([top, bottom])
    df = df.reset_index(drop=True)
    return df
