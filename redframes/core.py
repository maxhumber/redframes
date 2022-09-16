from __future__ import annotations

import pprint

from .checks import _check_type
from .types import (
    Any,
    Column,
    Columns,
    Direction,
    Func,
    Join,
    LazyColumns,
    NewColumn,
    NewValue,
    NumpyArray,
    OldColumn,
    OldValue,
    PandasDataFrame,
    PandasGroupedFrame,
    Value,
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
    """Internal/unsafe (no-copy) version of redframes.io.wrap()"""
    df = DataFrame()
    df._data = data
    return df


class _TakeMixin:
    def __init__(self, data: PandasDataFrame | PandasGroupedFrame) -> None:
        self._data = data

    def take(self, rows: int, **kwargs) -> DataFrame:
        """Take any number of rows from a DataFrame

        pandas: `head`, `tail`
        tidyverse: `slice_head`, `slice_tail`

        Examples:

        ```python
        df = rf.DataFrame({"foo": range(10)})
        ```
        |   foo |
        |------:|
        |     0 |
        |     1 |
        |     2 |
        |     3 |
        |     4 |
        |     5 |
        |     6 |
        |     7 |
        |     8 |
        |     9 |

        From "head":

        ```python
        df.take(1)
        ```
        |   foo |
        |------:|
        |     0 |

        From "tail":

        ```python
        df.take(-2)
        ```
        |   foo |
        |------:|
        |     8 |
        |     9 |
        """
        return _wrap(take(self._data, rows, **kwargs))


class _SKLearnMixin(_TakeMixin):
    def __init__(self, data: PandasDataFrame) -> None:
        self._data = data

    def __array__(self) -> NumpyArray:
        return self._data.__array__()

    def __len__(self) -> int:
        return self._data.__len__()

    @property
    def iloc(self):
        return self._data.iloc


class _CommonMixin(_TakeMixin):
    def __init__(self, data: PandasDataFrame | PandasGroupedFrame) -> None:
        self._data = data

    def accumulate(self, column: Column, into: Column) -> DataFrame:
        """Accumulate (cumsum) values in one column into another column

        pandas: `cumsum`
        tidyverse: `mutate(... = cumsum(...))`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2, 3, 4]})
        ```
        |   foo |
        |------:|
        |     1 |
        |     2 |
        |     3 |
        |     4 |

        ```python
        df.accumulate("foo", into="cumsum")
        ```
        |   foo |   cumsum |
        |------:|---------:|
        |     1 |        1 |
        |     2 |        3 |
        |     3 |        6 |
        |     4 |       10 |
        """
        return _wrap(accumulate(self._data, column, into))

    def rank(
        self,
        column: Column,
        into: Column,
        descending: bool = False,
    ) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(rank(self._data, column, into, descending))

    def summarize(self, over: dict[Column, tuple[Column, Func]]) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(summarize(self._data, over))


class GroupedFrame(_CommonMixin):
    def __repr__(self) -> str:
        return "GroupedFrame()"


class DataFrame(_CommonMixin, _SKLearnMixin):
    def __init__(self, data: dict[Column, Values] | None = None) -> None:
        _check_type(data, {dict, None})
        if not data:
            self._data = PandasDataFrame()
        if isinstance(data, dict):
            self._data = PandasDataFrame(data)

    def __eq__(self, rhs: Any) -> bool:
        _check_type(rhs, DataFrame)
        return self._data.equals(rhs._data)

    def __getitem__(self, key: Column) -> Values:
        return list(self._data[key])

    def __repr__(self) -> str:
        return self._data.__repr__()

    def _repr_html_(self) -> str:
        return self._data.to_html(index=True)  # index=False?

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
    def dimensions(self) -> dict[str, int]:
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
        """Concatenate another DataFrame to the bottom

        pandas: `concat`
        tidyverse: `bind_rows`

        Example:

        ```python
        df1 = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     2 | B     |

        ```python
        df2 = rf.DataFrame({"bar": ["C", "D"], "foo": [3, 4], "baz": ["$", "@"]})
        ```
        | bar   |   foo | baz   |
        |:------|------:|:------|
        | C     |     3 | $     |
        | D     |     4 | @     |

        ```python
        df1.append(df2)
        ```
        |   foo | bar   | baz   |
        |------:|:------|:------|
        |     1 | A     | nan   |
        |     2 | B     | nan   |
        |     3 | C     | $     |
        |     4 | D     | @     |
        """
        _check_type(other, DataFrame)
        return _wrap(append(self._data, other._data))

    def combine(
        self, columns: Columns, into: Column, sep: str = "-", drop: bool = True
    ) -> DataFrame:
        """Combine multiple columns into a single column (opposite of `separate`)

        pandas: `+`
        tidyverse: `unite`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     2 | B     |

        ```python
        df.combine(["bar", "foo"], into="baz", sep="::", drop=True)
        ```
        | baz   |
        |:------|
        | A::1  |
        | B::2  |
        """
        return _wrap(combine(self._data, columns, into, sep, drop))

    def dedupe(self, columns: LazyColumns | None = None) -> DataFrame:
        """Drop duplicate rows with reference to optional target columns

        pandas: `drop_duplicates`
        tidyverse: `distinct`

        Examples:

        ```python
        df = rf.DataFrame({"foo": [1, 1, 2, 2], "bar": ["A", "A", "B", "A"]})
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     1 | A     |
        |     2 | B     |
        |     2 | A     |

        All columns:

        ```python
        df.dedupe()
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     2 | B     |
        |     2 | A     |

        Single column:

        ```python
        df.dedupe("foo")
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     2 | B     |

        Multiple columns:

        ```python
        df.dedupe(["foo", "bar"])
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     2 | B     |
        |     2 | A     |
        """
        return _wrap(dedupe(self._data, columns))

    def denix(self, columns: LazyColumns | None = None) -> DataFrame:
        """Remove missing values with reference to optional target columns

        pandas: `dropna`
        tidyverse: `drop_na`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, None, 3, None, 5, 6], "bar": [1, None, 3, 4, None, None]})
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     1 |
        |   nan |   nan |
        |     3 |     3 |
        |   nan |     4 |
        |     5 |   nan |
        |     6 |   nan |

        All columns:

        ```python
        df.denix()
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     1 |
        |     3 |     3 |

        Single column:

        ```python
        df.denix("bar")
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     1 |
        |     3 |     3 |
        |   nan |     4 |

        Multiple columns:

        ```python
        df.denix(["foo", "bar"])
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     1 |
        |     3 |     3 |
        """
        return _wrap(denix(self._data, columns))

    def drop(self, columns: LazyColumns) -> DataFrame:
        """Drop specific columns (related: `select`)

        pandas: `drop(..., axis=1)`
        tidyverse: `select(- ...)`

        Examples:

        ```python
        rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
        ```
        |   foo |   bar |   baz |
        |------:|------:|------:|
        |     1 |     3 |     5 |
        |     2 |     4 |     6 |

        ```python
        df.drop("baz")
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     3 |
        |     2 |     4 |

        ```python
        df.drop(["foo", "baz"])
        ```
        |   bar |
        |------:|
        |     3 |
        |     4 |
        """
        return _wrap(drop(self._data, columns))

    def fill(
        self,
        columns: LazyColumns | None = None,
        direction: Direction | None = None,
        constant: Value | None = None,
    ) -> DataFrame:
        """Fill missing values "down", "up", or with a constant

        pandas: `fillna`
        tidyverse: `fill` / `replace_na`

        Examples:

        ```python
        df = rf.DataFrame({"foo": [1, None, None, 2, None], "bar": [None, "A", None, "B", None]})
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 |       |
        |   nan | A     |
        |   nan |       |
        |     2 | B     |
        |   nan |       |

        Constant (all columns):

        ```python
        df.fill(constant=0)
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | 0     |
        |     0 | A     |
        |     0 | 0     |
        |     2 | B     |
        |     0 | 0     |

        Down (all columns):

        ```python
        df.fill(direction="down")
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 |       |
        |     1 | A     |
        |     1 | A     |
        |     2 | B     |
        |     2 | B     |

        Down (single column):

        ```python
        df.fill("foo", direction="down")
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 |       |
        |     1 | A     |
        |     1 |       |
        |     2 | B     |
        |     2 |       |

        Up (single/mutiple columns):

        ```python
        df.fill(["foo"], direction="up")
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 |       |
        |     2 | A     |
        |     2 |       |
        |     2 | B     |
        |   nan |       |
        """
        return _wrap(fill(self._data, columns, direction, constant))

    def filter(self, func: Func) -> DataFrame:
        """Retain all rows that satisfy specific conditions

        `|`, `&`, `< <= == != >= >`, `isin`

        pandas: `df[df[col] == condition]`
        tidyverse: `filter`

        Examples:

        ```python
        df = rf.DataFrame({"foo": ["A", "A", "A", "B"], "bar": [1, 2, 3, 4]})
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | A     |     2 |
        | A     |     3 |
        | B     |     4 |

        Single condition:

        ```python
        df.filter(lambda row: row["foo"].isin(["A"]))
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | A     |     2 |
        | A     |     3 |

        And (multiple conditions):

        ```python
        df.filter(lambda row: (row["foo"] == "A") & (row["bar"] <= 2))
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | A     |     2 |

        Or (multiple conditions):

        ```python
        df.filter(lambda row: (row["foo"] == "B") | (row["bar"] == 1))
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | B     |     4 |
        """
        return _wrap(filter(self._data, func))

    def gather(
        self,
        columns: LazyColumns | None = None,
        into: tuple[Column, Column] = ("variable", "value"),
    ):
        """Lengthen data, by increasing rows and decreasing columns (opposite of `spread`)

        pandas: `melt`
        tidyverse: `gather`, `pivot_longer`

        Examples:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [4, 5]})
        ```
        |   foo |   bar |   baz |
        |------:|------:|------:|
        |     1 |     3 |     4 |
        |     2 |     4 |     5 | 

        All columns:

        ```python
        df.gather()
        ```
        | variable   |   value |
        |:-----------|--------:|
        | foo        |       1 |
        | foo        |       2 |
        | bar        |       3 |
        | bar        |       4 |
        | baz        |       4 |
        | baz        |       5 | 

        Single column:

        ```python
        df.gather("foo")
        ```
        |   bar |   baz | variable   |   value |
        |------:|------:|:-----------|--------:|
        |     3 |     4 | foo        |       1 |
        |     4 |     5 | foo        |       2 |

        Multiple columns:

        ```python
        df.gather(["foo", "bar"], into=("var", "val"))
        ```
        |   baz | var   |   val |
        |------:|:------|------:|
        |     4 | foo   |     1 |
        |     5 | foo   |     2 |
        |     4 | bar   |     3 |
        |     5 | bar   |     4 | 
        """
        return _wrap(gather(self._data, columns, into))

    def group(self, by: LazyColumns) -> GroupedFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return GroupedFrame(group(self._data, by))

    def join(
        self,
        rhs: DataFrame,
        on: LazyColumns,
        how: Join = "left",
    ) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        _check_type(rhs, DataFrame)
        return _wrap(join(self._data, rhs._data, on, how))

    def mutate(self, over: dict[Column, Func]) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(mutate(self._data, over))

    def rename(self, columns: dict[OldColumn, NewColumn]) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(rename(self._data, columns))

    def replace(self, over: dict[Column, dict[OldValue, NewValue]]) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(replace(self._data, over))

    def sample(self, rows: int | float = 1, seed: int | None = None) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(sample(self._data, rows, seed))

    def select(self, columns: LazyColumns) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(select(self._data, columns))

    def shuffle(self, seed: int | None = None) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(shuffle(self._data, seed))

    def sort(self, columns: LazyColumns, descending: bool = False) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(sort(self._data, columns, descending))

    def split(
        self, column: Column, into: Columns, sep: str, drop: bool = True
    ) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(split(self._data, column, into, sep, drop))

    def spread(self, column: Column, using: Column) -> DataFrame:
        """XXX

        pandas:
        tidyverse:

        Example:

        ```python

        ```
        """
        return _wrap(spread(self._data, column, using))
