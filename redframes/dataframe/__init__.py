from __future__ import annotations

import pprint

from ..types import (
    Any,
    Column,
    Columns,
    ColumnsU,
    Data,
    Dimensions,
    Direction,
    Func,
    Join,
    PandasCommonFrame,
    PandasDataFrame,
    Percent,
    Rank,
    Rows,
    Values,
)

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


def _wrap(data: PandasDataFrame) -> DataFrame:
    df = DataFrame()
    df._data = data
    return df


class _SKLearnMixin:
    def __init__(self, data: PandasDataFrame):
        self._data = data

    def __array__(self):
        return self._data.__array__()

    @property
    def iloc(self):
        return self._data.iloc


class _CommonFrameMixin:
    def __init__(self, data: PandasCommonFrame):
        self._data = data

    def accumulate(self, column: Column, into: Column) -> DataFrame:
        return _wrap(accumulate(self._data, column, into))

    def mutate(self, mutations: dict[Column, Func]) -> DataFrame:
        return _wrap(mutate(self._data, mutations))

    def rank(
        self,
        column: Column,
        into: Column,
        method: Rank = "dense",
        descending: bool = False,
    ) -> DataFrame:
        return _wrap(rank(self._data, column, into, method, descending))

    def summarize(
        self, into_over_funcs: dict[Column, tuple[Column, Func]]
    ) -> DataFrame:
        return _wrap(summarize(self._data, into_over_funcs))

    def take(self, rows: Rows = 1, **kwargs) -> DataFrame:
        return _wrap(take(self._data, rows, **kwargs))


class GroupedFrame(_CommonFrameMixin):
    def __repr__(self) -> str:
        return "GroupedFrame()"


class DataFrame(_CommonFrameMixin, _SKLearnMixin):
    def __init__(self, data: dict[Column, Values] | None = None):
        if not data:
            self._data = PandasDataFrame()
        elif isinstance(data, dict):
            self._data = PandasDataFrame(data)
        else:
            raise TypeError("must be dict[str, list[Any]] | None")

    def __eq__(self, rhs: Any) -> bool:
        if not isinstance(rhs, DataFrame):
            raise TypeError("must be rf.DataFrame")
        return self._data.equals(rhs._data)

    def __getitem__(self, key: Column) -> Values:
        return list(self._data[key])

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
    def columns(self) -> Columns:
        return list(self._data.columns)

    @property
    def dimensions(self) -> Dimensions:
        return dict(zip(["rows", "columns"], self._data.shape))

    @property
    def empty(self) -> bool:
        return self._data.empty

    @property
    def types(self) -> dict[Column, type]:
        data = self._data.astype("object")
        types = {str(col): type(data.loc[0, col]) for col in data}  # type: ignore
        return types

    def append(self, other: DataFrame) -> DataFrame:
        assert isinstance(other, DataFrame), "must be DataFrame"
        return _wrap(append(self._data, other._data))

    def combine(
        self, columns: Columns, into: Column, sep: str = "_", drop: bool = True
    ) -> DataFrame:
        return _wrap(combine(self._data, columns, into, sep, drop))

    def dedupe(self, columns: ColumnsU | None = None) -> DataFrame:
        return _wrap(dedupe(self._data, columns))

    def denix(self, columns: ColumnsU | None = None) -> DataFrame:
        return _wrap(denix(self._data, columns))

    def drop(self, columns: ColumnsU) -> DataFrame:
        return _wrap(drop(self._data, columns))

    def fill(
        self,
        columns: ColumnsU | None = None,
        direction: Direction | None = "down",
        constant: Any | None = None,
    ) -> DataFrame:
        return _wrap(fill(self._data, columns, direction, constant))

    def filter(self, func: Func) -> DataFrame:
        return _wrap(filter(self._data, func))

    def gather(
        self,
        columns: Columns | None = None,
        into: tuple[Column, Column] = ("variable", "value"),
    ):
        return _wrap(gather(self._data, columns, into))

    def group(self, by: ColumnsU) -> GroupedFrame:
        return GroupedFrame(group(self._data, by))

    def join(
        self,
        rhs: DataFrame,
        on: dict[Column, Column],
        how: Join = "left",
    ) -> DataFrame:
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs type is invalid")
        return _wrap(join(self._data, rhs._data, on, how))

    def rename(self, columns: dict[Column, Column]) -> DataFrame:
        return _wrap(rename(self._data, columns))

    def replace(self, rules: dict[Column, dict[Any, Any]]) -> DataFrame:
        return _wrap(replace(self._data, rules))

    def sample(self, rows: Rows | Percent = 1, seed: int | None = None) -> DataFrame:
        return _wrap(sample(self._data, rows, seed))

    def select(self, columns: ColumnsU) -> DataFrame:
        return _wrap(select(self._data, columns))

    def shuffle(self, seed: int | None = None) -> DataFrame:
        return _wrap(shuffle(self._data, seed))

    def sort(self, columns: ColumnsU, descending: bool = False) -> DataFrame:
        return _wrap(sort(self._data, columns, descending))

    def split(
        self, column: Column, into: Columns, sep: str, drop: bool = True
    ) -> DataFrame:
        return _wrap(split(self._data, column, into, sep, drop))

    def spread(self, column: Column, using: Column) -> DataFrame:
        return _wrap(spread(self._data, column, using))
