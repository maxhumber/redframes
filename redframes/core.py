from __future__ import annotations

import uuid
from typing import Any, Callable, Literal

import pandas as pd

from .verbs import take


def _valid_index(df: pd.DataFrame) -> bool:
    if not isinstance(df.index, pd.RangeIndex):
        return False
    if df.index.name:
        return False
    if df.iloc[0].name != 0:
        return False
    return True


def _valid_columns(df: pd.DataFrame) -> bool:
    if not isinstance(df.columns, pd.Index):
        return False
    if df.columns.has_duplicates:
        return False
    return True


def load(path: str, **kwargs) -> DataFrame:
    if (not isinstance(path, str)) or (not path.endswith(".csv")):
        raise TypeError(f"Invalid csv file at path ({path})")
    pdf = pd.read_csv(path, **kwargs)
    df = wrap(pdf, copy=False)
    return df

# def wrap(): 
#     pass

# def unwrap(): 
#     pass


def wrap(pdf: pd.DataFrame, **kwargs) -> DataFrame:
    df = DataFrame()
    if kwargs.get("copy") == False:
        df._data = pdf
    else:
        df._data = pdf.copy()
    return df


def unwrap(df: DataFrame, **kwargs) -> pd.DataFrame:
    if kwargs.get("copy") == False:
        return df._data
    else:
        return df._data.copy()


class DataFrame:
    def __init__(self, /, data: dict[str, list[Any]] | None = None):
        if not data:
            self._data = pd.DataFrame()
        elif isinstance(data, dict):
            self._data = pd.DataFrame(data)
        else:
            raise TypeError(f"Invalid type for input data ({type(data)})")

    def __eq__(self, rhs: object) -> bool:
        if not isinstance(rhs, DataFrame):
            raise NotImplementedError("__eq__ only works on rf.DataFrame")
        lhs, rhs = self._data, rhs._data
        return lhs.equals(rhs)

    def __getitem__(self, key: str) -> list[Any]:
        return list(self._data[key])

    def __repr__(self) -> str:
        return self._data.__repr__()

    def _repr_html_(self) -> str:
        return self._data._repr_html_()

    @property
    def shape(self) -> dict[str, int]:
        return dict(zip(["rows", "columns"], self._data.shape))

    @property
    def types(self) -> dict[str, type]:
        data = self._data
        data = data.reset_index(drop=True)
        data = data.astype("object")
        types = {str(col): type(data.loc[0, col]) for col in data}  # type: ignore
        return types

    @property
    def columns(self) -> list[str]:
        return list(self._data.columns)

    @property
    def rows(self) -> list[list[Any]]:
        return self._data.values.tolist()

    @property
    def empty(self) -> bool:
        return self._data.empty

    def take(self, /, rows: int = 1) -> DataFrame:
        data = unwrap(self)
        data = take(data, rows)
        return wrap(data)

    def slice(self, /, start: int, end: int) -> DataFrame:
        data = unwrap(self, copy=False)
        data = data.iloc[start:end]  # DEBATE: end+1?
        data = data.reset_index(drop=True)
        return wrap(data)

    def sample(self, /, rows: int | float = 1, *, seed: int | None = None) -> DataFrame:
        if type(rows) not in [int, float]:
            raise TypeError(f"Invalid rows argument ({type(rows)})")
        data = unwrap(self, copy=False)
        if rows >= 1:
            if isinstance(rows, float):
                raise ValueError("Rows argument must be an int if >= 1")
            data = data.sample(rows, random_state=seed)
        elif 0 < rows < 1:
            data = data.sample(frac=rows, random_state=seed)
        else:
            raise TypeError("rows must be a number >= 0")
        data = data.reset_index(drop=True)
        return wrap(data)

    def shuffle(self, *, seed: int | None = None) -> DataFrame:
        data = unwrap(self, copy=False)
        data = data.sample(frac=1, random_state=seed)
        data.reset_index(drop=True)
        return wrap(data)

    def sort(self, /, columns: list[str], *, reverse: bool = False) -> DataFrame:
        if not isinstance(columns, list):
            raise TypeError(f"Invalid columns argument ({type(columns)})")
        data = unwrap(self, copy=False)
        data = data.sort_values(by=columns, ascending=not reverse)
        data = data.reset_index(drop=True)
        return wrap(data)

    def filter(self, /, func: Callable[..., bool]) -> DataFrame:
        if not callable(func):
            raise TypeError("Must be a 'rowwise' function that returns a bool")
        data = unwrap(self, copy=False)
        data = data.loc[func]  # type: ignore
        data = data.reset_index(drop=True)
        return wrap(data)

    def dedupe(
        self,
        /,
        columns: list[str] | None = None,
        *,
        keep: Literal["first", "last"] = "first",
    ) -> DataFrame:
        if not (isinstance(columns, list) or not columns):
            raise TypeError(f"Invalid columns argument ({type(columns)})")
        if keep not in ["first", "last"]:  # DEBATE: remove keep option?
            raise ValueError("keep argument must be one of {'first', 'last'}")
        data = unwrap(self, copy=False)
        data = data.drop_duplicates(subset=columns, keep=keep)
        data = data.reset_index(drop=True)
        return wrap(data)

    def denix(self, /, columns: list[str] | None = None) -> DataFrame:
        if not (isinstance(columns, list) or not columns):
            raise TypeError(f"Invalid columns argument ({type(columns)})")
        data = unwrap(self, copy=False)
        data = data.dropna(subset=columns)
        data = data.reset_index(drop=True)
        return wrap(data)

    def fill(
        self,
        /,
        columns: list[str] | None = None,
        *,
        strategy: Literal["down", "up", "constant"] = "down",
        constant: str | int | float | None = None,
    ) -> DataFrame:
        if not (isinstance(columns, list) or not columns):
            raise TypeError(f"Invalid columns argument ({type(columns)})")
        try:
            method = {"down": "ffill", "up": "bfill", "constant": None}[strategy]
        except KeyError:
            raise ValueError(
                "Invalid strategy, must be one of {'down', 'up', 'constant}"
            )
        if strategy == "constant" and not constant:
            raise ValueError(
                "strategy='constant' requires a corresponding constant= argument"
            )
        data = unwrap(self)
        value = None if strategy in ["down", "up"] else constant
        if columns:
            data[columns] = data[columns].fillna(value=constant, method=method)  # type: ignore
        else:
            data = data.fillna(value=value, method=method)  # type: ignore
        return wrap(data)

    def replace(self, /, rules: dict[str, dict[Any, Any]]) -> DataFrame:
        if not isinstance(rules, dict):
            raise TypeError(f"Invalid rules type ({type(rules)})")
        bad_columns = list(set(rules.keys()) - set(self.columns))
        if bad_columns:
            raise KeyError(f"Invalid columns {bad_columns}")
        data = unwrap(self, copy=False)
        data = data.replace(rules)
        return wrap(data)

    def rename(self, /, columns: dict[str, str]) -> DataFrame:
        if not isinstance(columns, dict):
            raise TypeError(f"Invalid columns type ({type(columns)})")
        bad_columns = list(set(columns.keys()) - set(self.columns))
        if bad_columns:
            raise KeyError(f"Invalid columns {bad_columns}")
        data = unwrap(self, copy=False)
        data = data.rename(columns=columns)
        return wrap(data)

    def select(self, /, columns: list[str]) -> DataFrame:
        if not isinstance(columns, list):
            raise TypeError(f"Invalid columns type ({type(columns)})")
        bad_columns = list(set(columns) - set(self.columns))
        if bad_columns:
            raise KeyError(f"Invalid columns {bad_columns}")
        data = unwrap(self, copy=False)
        data = data[columns]
        return wrap(data)

    def remove(self, /, columns: list[str]) -> DataFrame:
        if not isinstance(columns, list):
            raise TypeError(f"Invalid columns type ({type(columns)})")
        data = unwrap(self, copy=False)
        data = data.drop(columns, axis=1)
        return wrap(data)

    def mutate(self, /, mutations: dict[str, Callable[..., Any]]) -> DataFrame:
        if not isinstance(mutations, dict):
            raise TypeError("Must be a dictionary of mutations")
        data = unwrap(self)
        for column, mutation in mutations.items():
            data[column] = data.apply(mutation, axis=1)
        return wrap(data)

    def split(self, /, column: str, *, sep: str, into: list[str]) -> DataFrame:
        if not isinstance(column, str):
            raise TypeError("column argument must be a string")
        if not isinstance(sep, str):
            raise TypeError("sep= separator must be a string")
        if not isinstance(into, list):
            raise TypeError("into= columns argument must be a list")
        data = unwrap(self)
        data[into] = data[column].str.split(sep, expand=True)
        return wrap(data)

    def combine(self, /, columns: list[str], *, sep: str = "_", into: str) -> DataFrame:
        if not isinstance(columns, list):
            raise TypeError("columns argument must be a list")
        if not isinstance(sep, str):
            raise TypeError("sep= separator must be a string")
        if not isinstance(into, str):
            raise TypeError("into= column argument must be a str")
        data = unwrap(self)
        new = uuid.uuid4().hex
        data[new] = data[columns].apply(
            lambda row: sep.join(row.values.astype(str)), axis=1
        )
        data = data.drop(columns, axis=1)
        data = data.rename(columns={new: into})
        return wrap(data)

    def append(self, /, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise TypeError("df argument must be a rf.DataFrame")
        top, bottom = unwrap(self, copy=False), unwrap(df, copy=False)
        data = pd.concat([top, bottom])
        data = data.reset_index(drop=True)
        return wrap(data)

    def join(
        self,
        /,
        rhs: DataFrame,
        *,
        on: dict[str, str],
        method: Literal["left", "right", "inner", "full"] = "left",
        suffixes=("_lhs", "_rhs"),
    ) -> DataFrame:
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs must be a rf.DataFrame")
        if not isinstance(on, dict):
            raise TypeError("on= argument must be a dict")
        if not method in ["left", "right", "inner", "full"]:
            raise TypeError(
                "method= argument must be one of {'left', 'right', 'inner', 'full'}"
            )
        how = "outer" if method == "full" else method
        lon, ron = list(on.keys()), list(on.values())
        ldf, rdf = unwrap(self, copy=False), unwrap(rhs, copy=False)
        data = pd.merge(ldf, rdf, left_on=lon, right_on=ron, how=how, suffixes=suffixes)
        data = data.reset_index(drop=True)
        return wrap(data)
