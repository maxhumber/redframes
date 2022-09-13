from ..checks import enforce
from ..types import Column, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def rename(df: PandasDataFrame, columns: dict[Column, Column]) -> PandasDataFrame:
    enforce(columns, dict)
    bad_columns = list(set(columns.keys()) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df.rename(columns=columns)
    return df
