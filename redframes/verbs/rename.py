from ..checks import _check_type, _check_values
from ..types import NewColumn, OldColumn, PandasDataFrame


def rename(df: PandasDataFrame, columns: dict[OldColumn, NewColumn]) -> PandasDataFrame:
    _check_type(columns, dict)
    cv = columns.values()
    _check_values(cv, str)
    if len(set(cv)) != len(cv):
        raise KeyError("columns must be unique")
    missing_keys = set(columns.keys()) - set(df.columns)
    if missing_keys and len(missing_keys) == 1:
        raise KeyError(f"column key ({missing_keys}) is invalid")
    if missing_keys and len(missing_keys) > 1:
        raise KeyError(f"column keys ({missing_keys}) are invalid")
    df = df.rename(columns=columns)
    return df
