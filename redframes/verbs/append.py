import pandas as pd

from ..types import PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns

def append(top: PandasDataFrame, bottom: PandasDataFrame) -> PandasDataFrame:
    df = pd.concat([top, bottom])
    df = df.reset_index(drop=True)
    return df
