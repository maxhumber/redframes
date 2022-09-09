from __future__ import annotations

from typing import Any, Callable, Literal

import pandas as pd
import pandas.core.groupby.generic as pg

from ..verbs import (
    accumulate,
    aggregate,
    append,
    combine,
    complete,
    dedupe,
    denix,
    fill,
    filter,
    gather,
    group, 
    join,
    mutate,
    rank,
    remove,
    rename,
    replace,
    sample,
    select,
    shuffle,
    slice,
    sort,
    split,
    spread,
    take,
)
from .magics import _eq, _getitem, _init, _repr, _repr_html
from .properties import columns, empty, rows, shape, types


def _wrap(data: pd.DataFrame) -> DataFrame:
    df = DataFrame()
    df._data = data
    return df


class CommonFrameMixin:
    def accumulate(
        self, /, column: str, *, method: Literal["min", "max", "sum"] = "sum", into: str
    ) -> DataFrame:
        data = accumulate(self._data, column, method, into)
        return _wrap(data)

    def aggregate(self, /, aggregations: dict[str, tuple[str, Callable[..., Any]]]) -> DataFrame:
        data = aggregate(self._data, aggregations)
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

    def take(self, /, rows: int = 1) -> DataFrame:
        data = take(self._data, rows)
        return _wrap(data)


class GroupedDataFrame(CommonFrameMixin):
    def __init__(self, /, data: pg.DataFrameGroupBy):
        self._data = data

    def __repr__(self) -> str:
        return "<GroupedDataFrame>"


class DataFrame(CommonFrameMixin):
    def __eq__(self, rhs: object) -> bool:
        if not isinstance(rhs, DataFrame):
            raise NotImplementedError("rhs type is invalid")
        return _eq(self._data, rhs._data)

    def __getitem__(self, key: str) -> list[Any]:
        return _getitem(self._data, key)

    def __init__(self, /, data: dict[str, list[Any]] | None = None):
        self._data = _init(data)

    def __repr__(self) -> str:
        return _repr(self._data)

    def _repr_html_(self) -> str:
        return _repr_html(self._data)

    @property
    def columns(self) -> list[str]:
        return columns(self._data)

    @property
    def empty(self) -> bool:
        return empty(self._data)

    @property
    def rows(self) -> list[list[Any]]:
        return rows(self._data)

    @property
    def shape(self) -> dict[str, int]:
        return shape(self._data)

    @property
    def types(self) -> dict[str, type]:
        return types(self._data)

    def append(self, /, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise TypeError("df type is invalid, must be rf.DataFrame")
        data = append(self._data, df._data)
        return _wrap(data)

    def combine(self, /, columns: list[str], *, sep: str = "_", into: str) -> DataFrame:
        data = combine(self._data, columns, sep, into)
        return _wrap(data)

    def complete(self, /, columns: list[str]) -> DataFrame:
        data = complete(self._data, columns)
        return _wrap(data)

    def dedupe(
        self,
        /,
        columns: list[str] | None = None,
        *,
        keep: Literal["first", "last"] = "first",
    ) -> DataFrame:
        data = dedupe(self._data, columns, keep)
        return _wrap(data)

    def denix(self, /, columns: list[str] | None = None) -> DataFrame:
        data = denix(self._data, columns)
        return _wrap(data)

    def fill(
        self,
        /,
        columns: list[str] | None = None,
        *,
        strategy: Literal["down", "up", "constant"] = "down",
        constant: str | int | float | None = None,
    ) -> DataFrame:
        data = fill(self._data, columns, strategy, constant)
        return _wrap(data)

    def filter(self, /, func: Callable[..., bool]) -> DataFrame:
        data = filter(self._data, func)
        return _wrap(data)

    def gather(
        self, /, columns: list[str], *, into: tuple[str, str] = ("variable", "value")
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
        method: Literal["left", "right", "inner", "full"] = "left",
        suffixes=("_lhs", "_rhs"),
    ) -> DataFrame:
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs type is invalid")
        data = join(self._data, rhs._data, on, method, suffixes)
        return _wrap(data)

    def remove(self, /, columns: list[str]) -> DataFrame:
        data = remove(self._data, columns)
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

    def slice(self, /, start: int, end: int) -> DataFrame:
        data = slice(self._data, start, end)
        return _wrap(data)

    def sort(self, /, columns: list[str], *, reverse: bool = False) -> DataFrame:
        data = sort(self._data, columns, reverse)
        return _wrap(data)

    def split(self, /, column: str, *, sep: str, into: list[str]) -> DataFrame:
        data = split(self._data, column, sep, into)
        return _wrap(data)

    def spread(self, /, column: str, using: str) -> DataFrame:
        data = spread(self._data, column, using)
        return _wrap(data)
