from .take import take

import itertools
from uuid import uuid4
import pandas as pd


class DataFrame:
    def __init__(self, data=pd.DataFrame()):
        """
        >>> df = DataFrame({"foo": [1, 2, 3]})
        >>> df
           foo
        0    1
        1    2
        2    3
        """
        if isinstance(data, pd.DataFrame):
            self._data = data
        elif isinstance(data, dict):
            self._data = pd.DataFrame(data)
        elif isinstance(data, str):
            if not data.endswith(".csv"):
                raise TypeError
            self._data = pd.read_csv(data)
        else:
            raise TypeError

    def __repr__(self):
        """
        >>> DataFrame({"foo": [0]})
           foo
        0    1
        """
        return self._data.__repr__()

    def _repr_html_(self):
        r"""
        >>> df = DataFrame({"foo": [1, 2, 3]})
        >>> df._repr_html_()[:50]
        '<div>\n<style scoped>\n    .dataframe tbody tr th:on'
        """
        return self._data._repr_html_()

    def __getitem__(self, key):
        """
        >>> df = DataFrame({"a": range(10)})
        >>> df["a"]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        return self._data[key].values.tolist()

    def __eq__(self, rhs):
        """
        >>> DataFrame({"a": range(10)}) == DataFrame({"a": range(10)})
        True
        """
        lhs, rhs = self._data, rhs._data
        return lhs.equals(rhs)

    @property
    def empty(self):
        """
        >>> df = DataFrame()
        >>> df.empty
        True
        """
        return self._data.empty

    @property
    def shape(self):
        """
        >>> df = DataFrame({"a": range(10)})
        >>> df.shape
        {'rows': 10, 'columns': 1}
        """
        return dict(zip(["rows", "columns"], self._data.shape))

    @property
    def columns(self):
        """
        >>> df = DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        >>> df.columns
        ['foo', 'bar']
        """
        return list(self._data.columns)

    @property
    def types(self):
        """
        >>> df = DataFrame({"foo": ["a", "b", "c"], "bar": [1, 2, 3], "baz": [4.0, None, 5.6]})
        >>> df.types
        {'foo': str, 'bar': numpy.int64, 'baz': numpy.float64}
        """
        data = self._data.reset_index(drop=True)
        return {col: type(data.loc[0, col]) for col in data}

    @property
    def values(self):
        """
        >>> df = DataFrame({"a": range(5), "b": range(5, 10), "c": range(10, 15)})
        >>> df.values
        [[0, 5, 10], [1, 6, 11], [2, 7, 12], [3, 8, 13], [4, 9, 14]]
        """
        return self._data.values.tolist()

    def take(self, rows=1):
        """
        >>> df = DataFrame({"numbers": range(100)})
        >>> df.take(1)
           numbers
        0        0
        >>> df.take(3)
           numbers
        0        0
        1        1
        2        2
        >>> print(df.take(-3))
            numbers
        97       97
        98       98
        99       99
        """
        data = take(self._data, rows=rows)
        return DataFrame(data)

    def sample(self, rows, *, seed=None):
        """
        >>> df = DataFrame({"foo": range(100)})
        >>> df.sample(5, seed=1)
            foo
        80   80
        84   84
        33   33
        81   81
        93   93
        >>> df.sample(0.03, seed=42)
            foo
        83   83
        53   53
        70   70
        """
        if rows >= 1:
            return DataFrame(self._data.sample(rows, random_state=seed))
        elif 0 < rows < 1:
            return DataFrame(self._data.sample(frac=rows, random_state=seed))
        else:
            raise TypeError("rows must be a number >= 0")

    def filter(self, func):
        """
        >>> df = DataFrame({"foo": [1, 2, 2, 2, 2, 3], "bar": [1, 2, 3, 4, 5, 6]})
        >>> df.filter(lambda d: d["bar"] >= 4)
           foo  bar
        3    2    4
        4    2    5
        5    3    6
        >>> df.filter(lambda d: (d["foo"] == 2) & (d["bar"] <= 4))
           foo  bar
        1    2    2
        2    2    3
        3    2    4
        """
        return DataFrame(self._data.loc[func])

    def sort(self, columns, *, reverse=False):
        """
        >>> df = DataFrame({"foo": [1, 1, 2, 2, 3], "bar": [1, -7, 5, 4, 5]})
        >>> df.sort(["bar"])
           foo  bar
        1    1   -7
        0    1    1
        3    2    4
        2    2    5
        4    3    5
        >>> df.sort(["bar"], reverse=True)
           foo  bar
        2    2    5
        4    3    5
        3    2    4
        0    1    1
        1    1   -7
        """
        return DataFrame(self._data.sort_values(by=columns, ascending=not reverse))

    def dedupe(self, columns=None):
        """
        >>> df = DataFrame({"foo": [1, 1, 1, 2, 2, 2], "bar": [1, 2, 3, 4, 5, 5]})
        >>> df.dedupe(["foo"])
           foo  bar
        0    1    1
        3    2    4
        >>> df.dedupe()
           foo  bar
        0    1    1
        1    1    2
        2    1    3
        3    2    4
        4    2    5
        """
        return DataFrame(self._data.drop_duplicates(subset=columns))

    def keep(self, columns):
        """
        >>> df = DataFrame({"a": [1], "b": [2], "c": [3]})
        >>> df = df.keep(["a", "b"])
        >>> df.columns
        ['a', 'b']
        """
        return DataFrame(self._data[columns])

    def discard(self, columns):
        """
        >>> df = DataFrame({"a": [1], "b": [2], "c": [3]})
        >>> df = df.discard(["c"])
        >>> df.columns
        ['a', 'b']
        """
        return DataFrame(self._data.drop(columns, axis=1))

    def rename(self, columns):
        """
        >>> df = DataFrame({"a": [1], "b": [2], "c": [3]})
        >>> df = df.rename({"a": "foo", "b": "bar"})
        >>> df.columns
        ['foo', 'bar', 'c']
        """
        return DataFrame(self._data.rename(columns=columns))

    def mutate(self, mutations):
        """
        >>> df = DataFrame({"foo": [1, 2, 3], "bar": ["A", "B", "C"]})
        >>> df = df.mutate({"baz": lambda row: row["foo"] * 5, "jaz": lambda row: d["baz"] / 4})
        >>> df
           foo bar  baz   jaz
        0    1   A    5  1.25
        1    2   B   10  2.50
        2    3   C   15  3.75
        """
        data = self._data.copy()
        for column, mutation in mutations.items():
            data[column] = data.apply(mutation, axis=1)
        return DataFrame(data)

    def dropna(self, columns=None):
        """
        >>> df = DataFrame({"foo": [0, 0, None, None, 0], "bar": [1, None, None, None, 1]})
        >>> df.dropna()
           foo  bar
        0  0.0  1.0
        4  0.0  1.0
        >>> df.dropna(["foo"])
           foo  bar
        0  0.0  1.0
        1  0.0  NaN
        4  0.0  1.0
        """
        return DataFrame(self._data.dropna(subset=columns))

    def join(self, rhs, columns, *, how="left", suffixes=("_lhs", "_rhs")):
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs is not of type DataFrame")
        if not isinstance(columns, list):
            raise TypeError("columns is not of type list")
        if not how in ["left", "right", "inner", "outer"]:
            raise TypeError("")
        lhs, rhs = self._data, rhs._data
        data = pd.merge(lhs, rhs, on=columns, how=how, suffixes=suffixes)
        return DataFrame(data)

    def append(self, df):
        top, bottom = self._data, df._data
        new = pd.concat([top, bottom])
        return DataFrame(new)

    def spread(self, column, using):
        data = self._data.copy()
        index = [col for col in data.columns if col not in [column, using]]
        data = pd.pivot_table(data, index=index, columns=[column], values=[using])
        data.columns = [col for col in data.columns.get_level_values(1)]
        data = data.reset_index().rename_axis(None, axis=0)
        return DataFrame(data)

    def gather(self, columns, into=("variable", "value"), *, dropna=True):
        data = self._data.copy()
        index = [col for col in data.columns if col not in columns]
        data = pd.melt(data, id_vars=index, value_vars=columns, var_name=into[0], value_name=into[1])
        data = data.dropna(subset="value")
        return DataFrame(data)

    def split(self, column, into, *, separator):
        data = self._data.copy()
        data[into] = data[column].str.split(separator, expand=True)
        return DataFrame(data)

    def combine(self, columns, into, *, separator="_"):
        data = self._data.copy()
        new = uuid4().hex
        data[new] = data[columns].apply(lambda row: separator.join(row.values.astype(str)), axis=1)
        data = data.drop(columns, axis=1)
        data = data.rename(columns={new: into})
        return DataFrame(data)

    def complete(self, columns):
        data = self._data.copy()
        series = [data[column] for column in columns]
        index = pd.MultiIndex.from_tuples(itertools.product(*series), names=columns)
        data = data.set_index(columns)
        data = data.reindex(index)
        data = data.reset_index()
        return DataFrame(data)

    def fill(self, columns=None, forward=True):
        data = self._data.copy()
        method = "ffill" if forward else "bfill"
        if columns:
            data[columns] = data[columns].fillna(method=method)
        else:
            data = data.fillna(method=method)
        return DataFrame(data)

    def slice(self, start, end):
        return DataFrame(self._data.iloc[start:end])

    def aggregate(self, apropos, by=None):
        data = self._data.copy()
        if by:
            data = data.groupby(by)
        data = data.agg(**apropos).reset_index()
        return DataFrame(data)

    def convert(self):
        return PandasDataFrame(self._data)

    def dump(self, path=None):
        data = self._data.copy()
        if not path:
            return data.to_dict(orient="list")
        if not path.endswith(".csv"):
            raise TypeError
        kwargs = {"index": False} | kwargs
        data.to_csv(path)
        return DataFrame(data)

class PandasDataFrame(pd.DataFrame):
    def convert(self):
        return DataFrame(self)


if __name__ == "__main__":
    import doctest
    doctest.testmod()