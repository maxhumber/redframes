from __future__ import annotations # remove at 3.10+

from typing import Any

import pandas as pd


class DataFrame:
    def __init__(
        self, /, data: str | dict[str, list[Any]] | pd.DataFrame = pd.DataFrame()
    ):
        if isinstance(data, str):
            if not data.endswith(".csv"):
                raise TypeError(f"\'{data}\' is not a '.csv'")
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
        data = self._data
        data = data.reset_index(drop=True)
        data = data.astype('object')
        types = {str(col): type(data.loc[0, col]) for col in data} # type: ignore
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
        data = self._data
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
        data = self._data
        data = data.iloc[start:end] # DEBATE: should this be +1?
        data = data.reset_index(drop=True)
        return DataFrame(data)

    def sample(self, /, rows: int | float = 1, *, seed: int = None) -> DataFrame:
        if type(rows) not in [int, float]:
            raise TypeError(f"Invalid rows argument ({type(rows)})")
        data = self._data
        if rows >= 1:
            data = data.sample(rows, random_state=seed)
        elif 0 < rows < 1:
            data = data.sample(frac=rows, random_state=seed)
        else:
            raise TypeError("rows must be a number >= 0")
        return DataFrame(data)