from ..types import Func, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def filter(df: PandasDataFrame, func: Func) -> PandasDataFrame:
    if not callable(func):
        raise TypeError("must be Func")
    df = df.loc[func]  # type: ignore
    df = df.reset_index(drop=True)
    return df
