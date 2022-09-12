from __future__ import annotations

import pprint
from typing import Any, Callable, Literal

import pandas as pd
import pandas.core.groupby.generic as pg

from .verbs import (
    accumulate,
    append,
    combine,
    dedupe,
    denix,
    drop,
    fill,
    filter,
    gather,
    group,
    join,
    mutate,
    rank,
    rename,
    replace,
    sample,
    select,
    shuffle,
    sort,
    split,
    spread,
    summarize,
    take,
)


def _wrap(data: pd.DataFrame) -> DataFrame:
    df = DataFrame()
    df._data = data
    return df


class _CommonFrameMixin:
    def __init__(self, /, data: pd.DataFrame | pg.DataFrameGroupBy):
        self._data = data

    def accumulate(self, /, column: str, *, into: str) -> DataFrame:
        data = accumulate(self._data, column, into)
        return _wrap(data)

    def mutate(self, /, mutations: dict[str, Callable[..., Any]]) -> DataFrame:
        data = mutate(self._data, mutations)
        return _wrap(data)

    def rank(
        self,
        /,
        column: str,
        *,
        method: Literal["average", "min", "max", "first", "dense"] = "dense",
        into: str,
        reverse: bool = False,
    ) -> DataFrame:
        data = rank(self._data, column, method, into, reverse)
        return _wrap(data)

    def summarize(
        self, /, into_over_funcs: dict[str, tuple[str, Callable[..., Any]]]
    ) -> DataFrame:
        data = summarize(self._data, into_over_funcs)
        return _wrap(data)

    def take(self, /, rows: int = 1, **kwargs) -> DataFrame:
        data = take(self._data, rows, **kwargs)
        return _wrap(data)


class GroupedDataFrame(_CommonFrameMixin):
    def __repr__(self) -> str:
        return "<GroupedDataFrame>"


class DataFrame(_CommonFrameMixin):
    def __array__(self):  # sklearn.* requirement
        return self._data.__array__()

    def __eq__(self, rhs: object) -> bool:
        if not isinstance(rhs, DataFrame):
            raise NotImplementedError("rhs type is invalid")
        return self._data.equals(self._data)

    def __getitem__(self, key: str) -> list[Any]:
        return list(self._data[key])

    def __init__(self, /, data: dict[str, list[Any]] | None = None):
        if not data:
            self._data = pd.DataFrame()
        elif isinstance(data, dict):
            self._data = pd.DataFrame(data)
        else:
            raise TypeError("data type is invalid")

    def __len__(self) -> int:
        return self._data.__len__()

    def __repr__(self) -> str:
        return self._data.__repr__()

    def _repr_html_(self) -> str:
        return self._data._repr_html_()

    def __str__(self) -> str:
        data = self._data.to_dict(orient="list")
        string = pprint.pformat(data, indent=4, sort_dicts=False, compact=True)
        if "\n" in string:
            string = " " + string[1:-1]
            string = f"rf.DataFrame({{\n{string}\n}})"
        else:
            string = f"rf.DataFrame({string})"
        return string

    @property
    def columns(self) -> list[str]:
        return list(self._data.columns)

    @property
    def dimensions(self) -> dict[str, int]:
        return dict(zip(["rows", "columns"], self._data.shape))

    @property
    def empty(self) -> bool:
        return self._data.empty

    # TODO: HIDE THIS
    @property
    def iloc(self):
        return self._data.iloc  # sklearn.model_selection.train_test_split requirement

    @property
    def rows(self) -> list[list[Any]]:
        return self._data.values.tolist()

    @property
    def types(self) -> dict[str, type]:
        data = self._data.astype("object")
        types = {str(col): type(data.loc[0, col]) for col in data}  # type: ignore
        return types

    def append(self, /, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise TypeError("df type is invalid, must be rf.DataFrame")
        data = append(self._data, df._data)
        return _wrap(data)

    def combine(
        self, /, columns: list[str], *, into: str, sep: str = "_", drop: bool = True
    ) -> DataFrame:
        data = combine(self._data, columns, into, sep, drop)
        return _wrap(data)

    def dedupe(self, /, columns: list[str] | None = None) -> DataFrame:
        data = dedupe(self._data, columns)
        return _wrap(data)

    def denix(self, /, columns: list[str] | None = None) -> DataFrame:
        data = denix(self._data, columns)
        return _wrap(data)

    def drop(self, /, columns: list[str]) -> DataFrame:
        data = drop(self._data, columns)
        return _wrap(data)

    def fill(
        self,
        /,
        columns: list[str] | None = None,
        *,
        direction: Literal["down", "up"] | None = "down",
        constant: str | int | float | None = None,
    ) -> DataFrame:
        data = fill(self._data, columns, direction, constant)
        return _wrap(data)

    def filter(self, /, func: Callable[..., bool]) -> DataFrame:
        data = filter(self._data, func)
        return _wrap(data)

    def gather(
        self,
        /,
        columns: list[str] | None = None,
        *,
        into: tuple[str, str] = ("variable", "value"),
    ):
        data = gather(self._data, columns, into)
        return _wrap(data)

    def group(self, /, columns: list[str]) -> GroupedDataFrame:
        data = group(self._data, columns)
        return GroupedDataFrame(data)

    def join(
        self,
        /,
        rhs: DataFrame,
        *,
        on: dict[str, str],
        how: Literal["left", "right", "inner", "full"] = "left",
    ) -> DataFrame:
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs type is invalid")
        data = join(self._data, rhs._data, on, how)
        return _wrap(data)

    def rename(self, /, columns: dict[str, str]) -> DataFrame:
        data = rename(self._data, columns)
        return _wrap(data)

    def replace(self, /, rules: dict[str, dict[Any, Any]]) -> DataFrame:
        data = replace(self._data, rules)
        return _wrap(data)

    def sample(self, /, rows: int | float = 1, *, seed: int | None = None) -> DataFrame:
        data = sample(self._data, rows, seed)
        return _wrap(data)

    def select(self, /, columns: list[str]) -> DataFrame:
        data = select(self._data, columns)
        return _wrap(data)

    def shuffle(self, *, seed: int | None = None) -> DataFrame:
        data = shuffle(self._data, seed)
        return _wrap(data)

    def sort(self, /, columns: list[str], *, reverse: bool = False) -> DataFrame:
        data = sort(self._data, columns, reverse)
        return _wrap(data)

    def split(
        self, /, column: str, *, into: list[str], sep: str, drop: bool = True
    ) -> DataFrame:
        data = split(self._data, column, into, sep, drop)
        return _wrap(data)

    def spread(self, /, column: str, using: str) -> DataFrame:
        data = spread(self._data, column, using)
        return _wrap(data)
