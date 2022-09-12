import json

from ..dataframe import DataFrame

# anatomize
# dissect
# serialize
# assay
# stringify
# str

def str(df: DataFrame, *, one: bool = False) -> str:
    if not isinstance(df, DataFrame):
        raise TypeError("df type is invalid, must be rf.DataFrame")
    if one: 
        df = df.take(1)
    data_dict = df._data.to_dict(orient="list")
    data_json = json.dumps(data_dict, indent=4)
    data_str = f"rf.DataFrame({data_json})"
    return data_str
