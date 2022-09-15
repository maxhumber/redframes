from ..checks import _check_type
from ..types import PandasDataFrame, NewColumn, OldColumn

# TODO: Test for Duplicate Columns


def rename(df: PandasDataFrame, columns: dict[OldColumn, NewColumn]) -> PandasDataFrame:
    _check_type(columns, dict)
    cv = columns.values()
    if len(set(cv)) != len(cv):
        raise KeyError("columns must be unique")
    missing_keys = set(columns.keys()) - set(df.columns)
    if missing_keys and len(missing_keys) == 1:
        raise KeyError(f"column key ({missing_keys}) is invalid")
    if missing_keys and len(missing_keys) > 1:
        raise KeyError(f"column keys ({missing_keys}) are invalid")
    df = df.rename(columns=columns)
    return df
