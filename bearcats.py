import pandas as pd

def load(path):
    """
    >>> df = load("example.csv")
    """
    if not isinstance(path, str):
        raise TypeError("path must type str")
    if not path.endswith(".csv"):
        raise TypeError("file at path must be a .csv")
    try:
        return DataFrame(pd.read_csv(path))
    except FileNotFoundError:
        raise FileNotFoundError(f"No such file or directory: '{path}'") from None


class DataFrame:
    def __init__(self, data=None):
        """
        >>> df = DataFrame({"foo": [1, 2, 3]})
        >>> df
           foo
        0    1
        1    2
        2    3
        """
        if data is None:
            self._pdf = pd.DataFrame()
        elif isinstance(data, dict):
            self._pdf = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            self._pdf = data
        else:
            raise TypeError("data must be a dict or pandas.DataFrame")

    def __repr__(self):
        """
        >>> DataFrame({"foo": [0]})
           foo
        0    1
        """
        return self._pdf.__repr__()

    def _repr_html_(self):
        r"""
        >>> df = DataFrame({"foo": [1, 2, 3]})
        >>> df._repr_html_()[:50]
        '<div>\n<style scoped>\n    .dataframe tbody tr th:on'
        """
        return self._pdf._repr_html_()

    def __getitem__(self, key):
        """
        >>> df = DataFrame({"a": range(10)})
        >>> df["a"]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        # return self._pdf[key]
        return self._pdf[key].values.tolist()

    def __eq__(self, rhs):
        """
        >>> DataFrame({"foo": [0]}) == DataFrame({"foo": [0]})
        True
        """
        return self._pdf == rhs._pdf

    @property
    def empty(self):
        """
        >>> df = DataFrame()
        >>> df.empty
        True
        """
        return self._pdf.empty

    @property
    def dimensions(self):
        """
        >>> df = DataFrame({"a": range(10)})
        >>> df.dimensions
        {'rows': 10, 'columns': 1}
        """
        return dict(zip(["rows", "columns"], self._pdf.shape))

    @property
    def columns(self):
        """
        >>> df = DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        >>> df.columns
        ['foo', 'bar']
        """
        return list(self._pdf.columns)

    @property
    def types(self):
        """
        >>> df = DataFrame({"foo": ["a", "b", "c"], "bar": [1, 2, 3], "baz": [4.0, None, 5.6]})
        >>> list(df.types)
        """
        df = self._pdf.reset_index(drop=True)
        return {col: type(df.loc[0, col]) for col in df}

    @property
    def values(self):
        """
        >>> df = DataFrame({"a": range(5), "b": range(5, 10), "c": range(10, 15)})
        >>> df.values
        [[0, 5, 10], [1, 6, 11], [2, 7, 12], [3, 8, 13], [4, 9, 14]]
        """
        return self._pdf.values.tolist()

    def head(self, rows=1):
        """
        >>> df = DataFrame({"numbers": range(100)})
        >>> df.head()
           numbers
        0        0
        >>> df.head(3)
           numbers
        0        0
        1        1
        2        2
        """
        return DataFrame(self._pdf.head(rows))

    def tail(self, rows=1):
        """
        >>> df = DataFrame({"numbers": range(100)})
        >>> print(df.tail())
            numbers
        99       99
        >>> print(df.tail(5))
            numbers
        95       95
        96       96
        97       97
        98       98
        99       99
        """
        return DataFrame(self._pdf.tail(rows))

    def sample(self, rows, /, seed=None):
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
            return DataFrame(self._pdf.sample(rows, random_state=seed))
        elif 0 < rows < 1:
            return DataFrame(self._pdf.sample(frac=rows, random_state=seed))
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
        return DataFrame(self._pdf.loc[func])

    def sort(self, columns, /, reverse=False):
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
        return DataFrame(self._pdf.sort_values(by=columns, ascending=not reverse))

    def dedupe(self, columns=None, /, keep="first"):
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
        assert keep in ["first", "last"], "keep must be 'first' or 'last'"
        return DataFrame(self._pdf.drop_duplicates(subset=columns, keep=keep))

    def select(self, columns):
        """
        >>> df = DataFrame({"a": [1], "b": [2], "c": [3]})
        >>> df = df.select(["a", "b"])
        >>> df.columns
        ['a', 'b']
        """
        return DataFrame(self._pdf[columns])

    def rename(self, columns):
        """
        >>> df = DataFrame({"a": [1], "b": [2], "c": [3]})
        >>> df = df.rename({"a": "foo", "b": "bar"})
        >>> df.columns
        ['foo', 'bar', 'c']
        """
        return DataFrame(self._pdf.rename(columns=columns))

    def mutate(self, mutations):
        """
        >>> df = DataFrame({"foo": [1, 2, 3], "bar": ["A", "B", "C"]})
        >>> df = df.mutate({"baz": lambda d: d["foo"] * 5, "jaz": lambda d: d["baz"] / 4})
        >>> df
           foo bar  baz   jaz
        0    1   A    5  1.25
        1    2   B   10  2.50
        2    3   C   15  3.75
        """
        pdf = self._pdf.copy()
        for column, mutation in mutations.items():
            pdf[column] = pdf.apply(mutation, axis=1)
        return DataFrame(pdf)

    def reindex(self):
        """
        >>> df = DataFrame({"foo": range(100)})
        >>> df.sample(5, seed=42).reindex()
           foo
        0   83
        1   53
        2   70
        3   45
        4   44
        """
        pdf = self._pdf.reset_index(drop=True)
        return DataFrame(pdf)

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
        pdf = self._pdf.dropna(subset=columns)
        return DataFrame(pdf)

    def join(self, rhs, columns, /, how="left", suffixes=("_lhs", "_rhs")):
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs is not of type DataFrame")
        if not isinstance(columns, list):
            raise TypeError("columns is not of type list")
        if not how in ["left", "right", "inner", "outer"]:
            raise TypeError("")
        lhs, rhs = self._pdf, rhs._pdf
        pdf = pd.merge(lhs, rhs, on=columns, how=how, suffixes=suffixes)
        return DataFrame(pdf)

    def append(self, df):
        top, bottom = self._pdf, df._pdf
        new = pd.concat([top, bottom])
        return DataFrame(new)

    def convert(self):
        return PandasDataFrame(self._pdf)

    def dump(path=None):
        pdf = self._pdf
        if not path:
            return pdf.to_dict(orient="list")
        if not path.endswith(".csv"):
            raise CustomError("Bad news")
        kwargs = {"index": False} | kwargs
        pdf.to_csv(path, *args, **kwargs)
        return DataFrame(pdf)

    # not ready
    # group + ungroup
    def reduce(self, groups, funcs):
        pdf = self._pdf.copy()
        pdf = pdf.groupby(groups).agg(funcs).reset_index()
        # pdf.columns = pdf.columns.to_flat_index()
        pdf.columns = [
            '_'.join([str(x)
                      for x in [y for y in item
                                if y]]) if not isinstance(item, str) else item
            for item in pdf.columns
        ]
        return DataFrame(pdf)

class PandasDataFrame(pd.DataFrame):
    def convert(self):
        return DataFrame(self)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
