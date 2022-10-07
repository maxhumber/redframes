from __future__ import annotations

import datetime
import pprint
import warnings

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
    NumpyType,
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
    cross,
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
    rollup,
    sample,
    select,
    shuffle,
    sort,
    split,
    spread,
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


class _InterchangeMixin(_TakeMixin):
    def __init__(self, data: PandasDataFrame) -> None:
        self._data = data

    def __array__(self) -> NumpyArray:
        return self._data.__array__()

    def __dataframe__(self, nan_as_null=False, allow_copy=True) -> "PandasDataFrameXchg":  # type: ignore
        return self._data.__dataframe__(nan_as_null, allow_copy)

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
        """Densely rank values in a specific column

        pandas: `rank("dense")`
        tidyverse: `dense_rank`

        Example:

        ```python
        df = rf.DataFrame({"foo": [2, 3, 3, 99, 1000, 1, -6, 4]})
        ```
        |   foo |
        |------:|
        |     2 |
        |     3 |
        |     3 |
        |    99 |
        |  1000 |
        |     1 |
        |    -6 |
        |     4 |

        ```python
        df.rank("foo", into="rank", descending=True)
        ```
        |   foo |   rank |
        |------:|-------:|
        |     2 |      5 |
        |     3 |      4 |
        |     3 |      4 |
        |    99 |      2 |
        |  1000 |      1 |
        |     1 |      6 |
        |    -6 |      7 |
        |     4 |      3 |
        """
        return _wrap(rank(self._data, column, into, descending))

    def rollup(self, over: dict[Column, tuple[Column, Func]]) -> DataFrame:
        """Apply summary functions/statistics to create new columns over specified columns

        pandas: `agg`
        tidyverse: `summarise`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2, 3, 4, 5], "bar": [99, 100, 1, -5, 2]})
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |    99 |
        |     2 |   100 |
        |     3 |     1 |
        |     4 |    -5 |
        |     5 |     2 |

        ```python
        df.rollup({
            "fcount": ("foo", rf.stat.count),
            "fmean": ("foo", rf.stat.mean),
            "fsum": ("foo", rf.stat.sum),
            "fmax": ("foo", rf.stat.max),
            "bmedian": ("bar", rf.stat.median),
            "bmin": ("bar", rf.stat.min),
            "bstd": ("bar", rf.stat.std)
        })
        ```
        |   fcount |   fmean |   fsum |   fmax |   bmedian |   bmin |   bstd |
        |---------:|--------:|-------:|-------:|----------:|-------:|-------:|
        |        5 |       3 |     15 |      5 |         2 |     -5 |  54.93 |
        """
        return _wrap(rollup(self._data, over))

    def summarize(self, over: dict[Column, tuple[Column, Func]]) -> DataFrame:
        message = "Marked for removal, please use `rollup` instead"
        warnings.warn(message, FutureWarning)
        return self.rollup(over)


class GroupedFrame(_CommonMixin):
    """GroupedFrame compatible with: `accumulate`, `rank`, `rollup`, `take`"""

    def __repr__(self) -> str:
        return "GroupedFrame()"


class DataFrame(_CommonMixin, _InterchangeMixin):
    def __init__(self, data: dict[Column, Values] | None = None) -> None:
        """Initialize a DataFrame with a standard dictionary

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | A     |
        |     2 | B     |
        """
        _check_type(data, {dict, None})
        if not data:
            self._data = PandasDataFrame()
        if isinstance(data, dict):
            self._data = PandasDataFrame(data)

    def __eq__(self, rhs: Any) -> bool:
        """Check if two DataFrames are equal to eachother

        Example:

        ```python
        adf = rf.DataFrame({"foo": [1]})
        bdf = rf.DataFrame({"bar": [1]})
        cdf = rf.DataFrame({"foo": [1]})
        print(adf == bdf)
        print(adf == cdf)
        # False
        # True
        ```
        """
        if not isinstance(rhs, DataFrame):
            return False
        return self._data.equals(rhs._data)

    def __getitem__(self, key: Column) -> Values:
        """Retrive values (as a python list) from a specified column

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        df["foo"]
        # [1, 2]
        ```
        """
        return list(self._data[key])

    def __repr__(self) -> str:
        return self._data.__repr__()

    def _repr_html_(self) -> str:
        return self._data.to_html(index=True)  # index=False?

    def __str__(self) -> str:
        """Return string constructor (for copy-and-pasting)

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        str(df)
        # "rf.DataFrame({'foo': [1, 2], 'bar': ['A', 'B']})"
        ```
        """
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
        """Inspect column names (keys)

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"], "baz": [True, False]})
        df.columns
        # ['foo', 'bar', 'baz']
        ```
        """
        return list(self._data.columns)

    @property
    def dimensions(self) -> dict[str, int]:
        """Inspect DataFrame shape

        Example:

        ```python
        df = rf.DataFrame({"foo": range(10), "bar": range(10, 20)})
        df.dimensions
        # {'rows': 10, 'columns': 2}
        ```
        """
        return dict(zip(["rows", "columns"], self._data.shape))

    @property
    def empty(self) -> bool:
        """Inspect if DataFrame is "empty"

        Example:

        ```python
        df = rf.DataFrame()
        df.empty
        # True
        ```
        """
        return self._data.empty

    @property
    def memory(self) -> str:
        """Interrogate DataFrame (deep) memory usage

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": ["A", "B", "C"]})
        df.memory
        # '326B'
        ```
        """
        size = self._data.memory_usage(deep=True).sum()
        power_labels = {40: "TB", 30: "GB", 20: "MB", 10: "KB"}
        for power, label in power_labels.items():
            if size >= (2**power):
                approx_size = size // 2**power
                return f"{approx_size} {label}"
        return f"{size} B"

    @property
    def types(self) -> dict[Column, type]:
        """Inspect column types

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"], "baz": [True, False]})
        df.types
        # {'foo': int, 'bar': object, 'baz': bool}
        ```
        """
        numpy_types = {
            NumpyType("O"): object,
            NumpyType("int64"): int,
            NumpyType("float64"): float,
            NumpyType("bool"): bool,
            NumpyType("datetime64"): datetime.datetime,
        }
        raw_types = dict(self._data.dtypes)
        clean_types = {}
        for column in self.columns:
            current = raw_types[column]
            clean = numpy_types.get(current, current)  # type: ignore
            clean_types[column] = clean
        return clean_types

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
        self, columns: Columns, into: Column, sep: str, drop: bool = True
    ) -> DataFrame:
        """Combine multiple columns into a single column (opposite of `split`)

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

    def cross(
        self, rhs: DataFrame | None = None, postfix: tuple[str, str] = ("_lhs", "_rhs")
    ) -> DataFrame:
        """Cross join another DataFrame (or the DataFrame itself)

        pandas: `merge(..., how="cross")`
        tidyverse: `full_join(..., by = character())`

        Examples:

        ```python
        df = rf.DataFrame({"foo": ["a", "b", "c"], "bar": [1, 2, 3]})
        ```
        | foo   |   bar |
        |:------|------:|
        | a     |     1 |
        | b     |     2 |
        | c     |     3 |

        Self:

        ```python
        df.cross()
        ```

        | foo_lhs   |   bar_lhs | foo_rhs   |   bar_rhs |
        |:----------|----------:|:----------|----------:|
        | a         |         1 | a         |         1 |
        | a         |         1 | b         |         2 |
        | a         |         1 | c         |         3 |
        | b         |         2 | a         |         1 |
        | b         |         2 | b         |         2 |
        | b         |         2 | c         |         3 |
        | c         |         3 | a         |         1 |
        | c         |         3 | b         |         2 |
        | c         |         3 | c         |         3 |

        Two DataFrames:

        ```python
        dfa = rf.DataFrame({"foo": [1, 2, 3]})
        dfb = rf.DataFrame({"bar": [1, 2, 3]})
        dfa.cross(dfb, postfix=("_a", "_b"))
        ```

        |   foo |   bar |
        |------:|------:|
        |     1 |     1 |
        |     1 |     2 |
        |     1 |     3 |
        |     2 |     1 |
        |     2 |     2 |
        |     2 |     3 |
        |     3 |     1 |
        |     3 |     2 |
        |     3 |     3 |
        """
        rhs = self if (rhs == None) else rhs
        _check_type(rhs, DataFrame)
        return _wrap(cross(self._data, rhs._data, postfix))

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
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
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
        tidyverse: `fill`, `replace_na`

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
        columns: Columns | None = None,
        into: tuple[Column, Column] = ("variable", "value"),
    ):
        """Lengthen data, increase rows, decrease columns (opposite of `spread`)

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
        """Create a GroupedFrame overwhich split-apply-combine operations can be applied

        Compatible verbs: `accumulate`, `rank`, `rollup`, `take`

        pandas: `groupby`
        tidyverse: `group_by`

        Example:

        ```python
        df = rf.DataFrame({"foo": ["A", "A", "A", "B", "B"], "bar": [1, 2, 3, 4, 5], "baz": [9, 7, 7, 5, 6]})
        ```
        | foo   |   bar |   baz |
        |:------|------:|------:|
        | A     |     1 |     9 |
        | A     |     2 |     7 |
        | A     |     3 |     7 |
        | B     |     4 |     5 |
        | B     |     5 |     6 |

        + `accumulate`:

        ```python
        df.group("foo").accumulate("bar", into="bar_cumsum")
        ```
        | foo   |   bar |   baz |   bar_cumsum |
        |:------|------:|------:|-------------:|
        | A     |     1 |     9 |            1 |
        | A     |     2 |     7 |            3 |
        | A     |     3 |     7 |            6 |
        | B     |     4 |     5 |            4 |
        | B     |     5 |     6 |            9 |

        + `rank`:

        ```python
        df.group("foo").rank("baz", into="baz_rank", descending=True)
        ```
        | foo   |   bar |   baz |   baz_rank |
        |:------|------:|------:|-----------:|
        | A     |     1 |     9 |          1 |
        | A     |     2 |     7 |          2 |
        | A     |     3 |     7 |          2 |
        | B     |     4 |     5 |          2 |
        | B     |     5 |     6 |          1 |

        + `rollup`:

        ```python
        df.group("foo").rollup({
            "bar_mean": ("bar", rf.stat.mean),
            "baz_min": ("baz", rf.stat.min)
        })
        ```
        | foo   |   bar_mean |   baz_min |
        |:------|-----------:|----------:|
        | A     |        2   |         7 |
        | B     |        4.5 |         5 |

        + `take`:

        ```python
        df.group("foo").take(1)
        ```
        | foo   |   bar |   baz |
        |:------|------:|------:|
        | A     |     1 |     9 |
        | B     |     4 |     5 |

        """
        return GroupedFrame(group(self._data, by))

    def join(
        self,
        rhs: DataFrame,
        on: LazyColumns,
        how: Join = "left",
        postfix: tuple[str, str] = ("_lhs", "_rhs"),
    ) -> DataFrame:
        """Join two DataFrames together using specific matching columns

        pandas: `merge`
        tidyverse: `left_join`, `right_join`, `inner_join`, `full_join`

        Examples:

        ```python
        adf = rf.DataFrame({"foo": ["A", "B", "C"], "bar": [1, 2, 3]})
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | B     |     2 |
        | C     |     3 |

        ```python
        bdf = rf.DataFrame({"foo": ["A", "B", "D"], "baz": ["!", "@", "#"]})
        ```
        | foo   | baz   |
        |:------|:------|
        | A     | !     |
        | B     | @     |
        | D     | #     |

        Left join:

        ```python
        adf.join(bdf, on="foo", how="left")
        ```
        | foo   |   bar | baz   |
        |:------|------:|:------|
        | A     |     1 | !     |
        | B     |     2 | @     |
        | C     |     3 | nan   |

        Right join:

        ```python
        adf.join(bdf, on="foo", how="right")
        ```
        | foo   |   bar | baz   |
        |:------|------:|:------|
        | A     |     1 | !     |
        | B     |     2 | @     |
        | D     |   nan | #     |

        Inner join:

        ```python
        adf.join(bdf, on="foo", how="inner")
        ```
        | foo   |   bar | baz   |
        |:------|------:|:------|
        | A     |     1 | !     |
        | B     |     2 | @     |

        Full join:

        ```python
        adf.join(bdf, on="foo", how="full")
        ```
        | foo   |   bar | baz   |
        |:------|------:|:------|
        | A     |     1 | !     |
        | B     |     2 | @     |
        | C     |     3 | nan   |
        | D     |   nan | #     |
        """
        _check_type(rhs, DataFrame)
        return _wrap(join(self._data, rhs._data, on, how, postfix))

    def mutate(self, over: dict[Column, Func]) -> DataFrame:
        """Create new, or transform existing columns

        pandas: `apply`, `astype`
        tidyverse: `mutate`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2, 3]})
        ```
        |   foo |
        |------:|
        |     1 |
        |     2 |
        |     3 |

        ```python
        df.mutate({
            "bar": lambda row: float(row["foo"]),
            "baz": lambda row: "X" + str(row["bar"] * 2),
            "jaz": lambda _: "Jazz"
        })
        ```
        |   foo |   bar | baz   | jaz   |
        |------:|------:|:------|:------|
        |     1 |     1 | X2.0  | Jazz  |
        |     2 |     2 | X4.0  | Jazz  |
        |     3 |     3 | X6.0  | Jazz  |
        """
        return _wrap(mutate(self._data, over))

    def rename(self, columns: dict[OldColumn, NewColumn]) -> DataFrame:
        """Rename columns from "Old" to "New"

        pandas: `rename`
        tidyverse: `rename`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     3 |
        |     2 |     4 |

        ```python
        df.rename({"foo": "oof", "bar": "rab"})
        ```
        |   oof |   rab |
        |------:|------:|
        |     1 |     3 |
        |     2 |     4 |

        """
        return _wrap(rename(self._data, columns))

    def replace(self, over: dict[Column, dict[OldValue, NewValue]]) -> DataFrame:
        """Replace values in specified columns, from "old" to "new"

        pandas: `replace`
        tidyverse: `mutate(... = case_when(...))`

        Example:

        ```python
        df = rf.DataFrame({"foo": [1, 2, 2, 2, 1], "bar": [1, "A", "B", True, False]})
        ```
        |   foo | bar   |
        |------:|:------|
        |     1 | 1     |
        |     2 | A     |
        |     2 | B     |
        |     2 | True  |
        |     1 | False |

        ```python
        df.replace({
            "foo": {2: 222},
            "bar": {False: 0, True: 1, "A": 2, "B": 3}
        })
        ```
        |   foo |   bar |
        |------:|------:|
        |     1 |     1 |
        |   222 |     2 |
        |   222 |     3 |
        |   222 |     1 |
        |     1 |     0 |
        """
        return _wrap(replace(self._data, over))

    def sample(self, rows: int | float, seed: int | None = None) -> DataFrame:
        """Sample any number, or percentage total of rows

        pandas: `sample(n, frac)`
        tidyverse: `sample_n`, `sample_frac`

        Examples:

        ```python
        df = rf.DataFrame({"foo": range(10), "bar": range(10, 20)})
        ```
        |   foo |   bar |
        |------:|------:|
        |     0 |    10 |
        |     1 |    11 |
        |     2 |    12 |
        |     3 |    13 |
        |     4 |    14 |
        |     5 |    15 |
        |     6 |    16 |
        |     7 |    17 |
        |     8 |    18 |
        |     9 |    19 |

        Single row:

        ```python
        df.sample(1)
        ```
        |   foo |   bar |
        |------:|------:|
        |     7 |    17 |

        Multiple rows:

        ```python
        df.sample(3)
        ```
        |   foo |   bar |
        |------:|------:|
        |     4 |    14 |
        |     1 |    11 |
        |     6 |    16 |

        Percentage of total rows (30%):

        ```python
        df.sample(0.3)
        ```
        |   foo |   bar |
        |------:|------:|
        |     4 |    14 |
        |     3 |    13 |
        |     1 |    11 |
        """
        return _wrap(sample(self._data, rows, seed))

    def select(self, columns: LazyColumns) -> DataFrame:
        """Retain specified columns (opposite of `drop`)

        pandas: `select`
        tidyverse: `select`

        Examples:

        ```python
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
        ```
        |   foo |   bar |   baz |
        |------:|------:|------:|
        |     1 |     3 |     5 |
        |     2 |     4 |     6 |

        Single column:

        ```python
        df.select("foo")
        ```
        |   foo |
        |------:|
        |     1 |
        |     2 |

        Multiple columns:

        ```python
        df.select(["foo", "baz"])
        ```
        |   foo |   baz |
        |------:|------:|
        |     1 |     5 |
        |     2 |     6 |
        """
        return _wrap(select(self._data, columns))

    def shuffle(self, seed: int | None = None) -> DataFrame:
        """Shuffle all rows in DataFrame

        pandas: `sample(frac=1)`
        tidyverse: `sample_frac(..., 1)`

        Example:

        ```python
        df = rf.DataFrame({"foo": range(5), "bar": range(5, 10)})
        ```
        |   foo |   bar |
        |------:|------:|
        |     0 |     5 |
        |     1 |     6 |
        |     2 |     7 |
        |     3 |     8 |
        |     4 |     9 |

        ```python
        df.shuffle()
        ```
        |   foo |   bar |
        |------:|------:|
        |     4 |     9 |
        |     2 |     7 |
        |     3 |     8 |
        |     0 |     5 |
        |     1 |     6 |
        """
        return _wrap(shuffle(self._data, seed))

    def sort(self, columns: LazyColumns, descending: bool = False) -> DataFrame:
        """Order rows with reference to specific columns

        pandas: `sort_values`
        tidyverse: `arrange`

        Examples:

        ```python
        df = rf.DataFrame({"foo": ["Z", "X", "A", "A"], "bar": [2, -2, 4, -4]})
        ```
        | foo   |   bar |
        |:------|------:|
        | Z     |     2 |
        | X     |    -2 |
        | A     |     4 |
        | A     |    -4 |

        Single column:

        ```python
        df.sort("bar")
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |    -4 |
        | X     |    -2 |
        | Z     |     2 |
        | A     |     4 |

        Descending order:

        ```python
        df.sort("bar", descending=True)
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     4 |
        | Z     |     2 |
        | X     |    -2 |
        | A     |    -4 |

        Multiple columns:

        ```python
        df.sort(["foo", "bar"], descending=False)
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |    -4 |
        | A     |     4 |
        | X     |    -2 |
        | Z     |     2 |
        """
        return _wrap(sort(self._data, columns, descending))

    def split(
        self, column: Column, into: Columns, sep: str, drop: bool = True
    ) -> DataFrame:
        """Split a column into multiple columns (opposite of `combine`)

        pandas: `df[col].str.split()`
        tidyverse: `separate`

        Example:

        ```python
        df = rf.DataFrame({"foo": ["A::1", "B::2", "C:3"]})
        ```
        | foo   |
        |:------|
        | A::1  |
        | B::2  |
        | C:3   |

        ```python
        df.split("foo", into=["foo", "bar"], sep="::", drop=True)
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | B     |     2 |
        | C:3   |       |
        """
        return _wrap(split(self._data, column, into, sep, drop))

    def spread(self, column: Column, using: Column) -> DataFrame:
        """Widen data, increase columns, decreas rows (opposite of `gather`)

        pandas: `pivot_table`
        tidyverse: `spread`, `pivot_wider`

        Example:

        ```python
        df = rf.DataFrame({"foo": ["A", "A", "A", "B", "B", "B", "B"], "bar": [1, 2, 3, 4, 5, 6, 7]})
        ```
        | foo   |   bar |
        |:------|------:|
        | A     |     1 |
        | A     |     2 |
        | A     |     3 |
        | B     |     4 |
        | B     |     5 |
        | B     |     6 |
        | B     |     7 |

        ```python
        df.spread("foo", using="bar")
        ```
        |   A |   B |
        |----:|----:|
        |   1 |   4 |
        |   2 |   5 |
        |   3 |   6 |
        | nan |   7 |
        """
        return _wrap(spread(self._data, column, using))
