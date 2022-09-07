from __future__ import annotations  # REMOVE: 3.10+

from typing import Any, Callable, Literal

import pandas as pd


class DataFrame:
    def __init__(
        self, /, data: str | dict[str, list[Any]] | pd.DataFrame = pd.DataFrame()
    ):
        if isinstance(data, str):
            if not data.endswith(".csv"):
                raise TypeError(f"'{data}' is not a '.csv'")
            self._data = pd.read_csv(data)
        elif isinstance(data, dict):
            self._data = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            self._data = data
        else:
            raise TypeError(f"Invalid data input type ({type(data)})")

    def __repr__(self) -> str:
        return self._data.__repr__()

    def _repr_html_(self) -> str:
        return self._data._repr_html_()

    def __getitem__(self, key: str) -> list[Any]:
        return list(self._data[key])

    def __eq__(self, rhs: object) -> bool:
        if not isinstance(rhs, DataFrame):
            raise NotImplementedError("__eq__ is just for checking rf.DataFrame")
        lhs, rhs = self._data, rhs._data
        return lhs.equals(rhs)

    @property
    def shape(self) -> dict[str, int]:
        return dict(zip(["rows", "columns"], self._data.shape))

    @property
    def types(self) -> dict[str, Any]:
        data = self._data.copy()
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
        if not isinstance(rows, int):
            raise TypeError(f"Invalid rows argument ({type(rows)})")
        data = self._data.copy()
        if rows > data.shape[0]:
            raise ValueError("Rows argument exceeds total number of rows")
        if rows == 0:
            raise ValueError("Rows argument must not be 0")
        if rows <= -1:
            data = data.tail(rows * -1)
        else:
            data = data.head(rows)
        data = data.reset_index(drop=True)
        return DataFrame(data)

    def slice(self, /, start: int, end: int) -> DataFrame:
        data = self._data.copy()
        data = data.iloc[start:end]  # DEBATE: end+1?
        data = data.reset_index(drop=True)
        return DataFrame(data)

    def sample(self, /, rows: int | float = 1, *, seed: int | None = None) -> DataFrame:
        if type(rows) not in [int, float]:
            raise TypeError(f"Invalid rows argument ({type(rows)})")
        data = self._data.copy()
        if rows >= 1:
            if isinstance(rows, float):
                raise ValueError("Rows argument must be an int if >= 1")
            data = data.sample(rows, random_state=seed)
        elif 0 < rows < 1:
            data = data.sample(frac=rows, random_state=seed)
        else:
            raise TypeError("rows must be a number >= 0")
        data = data.reset_index(drop=True)
        return DataFrame(data)

    def shuffle(self, *, seed: int | None = None) -> DataFrame:
        data = self._data.copy()
        data = data.sample(frac=1, random_state=seed)
        data.reset_index(drop=True)
        return DataFrame(data)

    def sort(self, /, columns: list[str], *, reverse: bool = False) -> DataFrame:
        if not isinstance(columns, list):
            raise TypeError(f"Invalid columns argument ({type(columns)})")
        data = self._data.copy()
        data = data.sort_values(by=columns, ascending=not reverse)
        data = data.reset_index(drop=True)
        return DataFrame(data)

    def filter(self, /, func: Callable[..., bool]) -> DataFrame:
        if not callable(func):
            raise TypeError("Must 'rowwise' function that returns a bool")
        data = self._data.copy()
        data = data.loc[func]  # type: ignore
        data = data.reset_index(drop=True)
        return DataFrame(data)

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
        data = self._data.copy()
        data = data.drop_duplicates(subset=columns, keep=keep)
        data = data.reset_index(drop=True)
        return DataFrame(data)

    def sanitize(self, /, columns: list[str] | None = None) -> DataFrame:
        if not (isinstance(columns, list) or not columns):
            raise TypeError(f"Invalid columns argument ({type(columns)})")
        data = self._data.copy()
        data = data.dropna(subset=columns)
        data = data.reset_index(drop=True)
        return DataFrame(data)

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
            raise ValueError("Invalid strategy, must be one of {'down', 'up', 'constant}")
        if strategy == "constant" and not constant:
            raise ValueError("strategy='constant' requires a corresponding constant= argument")
        data = self._data.copy()
        value = None if strategy in ["down", "up"] else constant
        if columns:
            data[columns] = data[columns].fillna(value=constant, method=method)
        else:
            data = data.fillna(value=value, method=method)
        return DataFrame(data)