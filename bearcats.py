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
        return self._pdf.__repr__()

    def _repr_html_(self):
        """
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
        return list(self._pdf[key].values)

    @property
    def columns(self):
        """
        >>> df = DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        >>> df.columns
        ['foo', 'bar']
        """
        return list(self._pdf.columns)

    @property
    def dimensions(self):
        """
        >>> df = DataFrame({"a": range(10)})
        >>> df.dimensions
        {'rows': 10, 'columns': 1}
        """
        return dict(zip(["rows", "columns"], self._pdf.shape))

    @property
    def values(self):
        """
        >>> df = DataFrame({"a": range(5), "b": range(5, 10), "c": range(10, 15)})
        df.values
        """
        return self._pdf.values.tolist()

    def head(self, rows=1):
        """
        >>> df = DataFrame({"numbers": range(100)})
        >>> df.head()
             a
        0    0
        >>> df.head(2)
             a
        0    0
        1    1
        """
        return DataFrame(self._pdf.head(rows))

    def tail(self, rows=1):
        """
        >>> df = DataFrame({"numbers": range(100)})
        >>> df.tail()
            numbers
        99    99
        >>> df.tail(5)
            numbers
        95    95
        96    96
        97    97
        98    98
        99    99
        """
        return DataFrame(self._pdf.tail(rows))

    def sample(self, rows):
        if rows >= 1:
            return DataFrame(self._pdf.sample(rows))
        elif 0 < rows < 1:
            return DataFrame(self._pdf.sample_frac(rows))
        else:
            raise TypeError("rows must be a number >= 0")

    def filter(self, func):
        """
        >>> df = DataFrame({"foo": [1, 1, 2, 2, 3], "bar": [1, -7, 5, 4, 5]})
        >>> df.filter(lambda d: (d["foo"] == 2) & (d["bar"] == 5))
        	foo	bar
        2	2	5
        """
        return DataFrame(self._pdf.loc[func])

    def sort(self, columns, reverse=False):
        """
        >>> df = DataFrame({"foo": [1, 1, 2, 2, 3], "bar": [1, -7, 5, 4, 5]})
        >>> df.sort(["bar"])
            foo	bar
        1	1	-7
        0	1	1
        3	2	4
        2	2	5
        4	3	5
        """
        return DataFrame(self._pdf.sort_values(by=columns, ascending=not reverse))

    def dedupe(self, columns=None, keep="first"):
        """
        >>> df = DataFrame({"foo": [1, 1, 2, 2, 3], "bar": [1, 2, 3, 4, 5]})
        >>> df.dedupe(["foo"])
        	foo	bar
        0	1	1
        2	2	3
        4	3	5
        """
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
        >>> df.mutate({"baz": lambda d: d["foo"] * 5, "jaz": lambda d: d["baz"] / 4})
        df.mutate({"foo": lambda d: str(d["foo"])})
        """
        pdf = self._pdf.copy()
        for column, mutation in mutations.items():
            pdf[column] = pdf.apply(mutation, axis=1)
        return DataFrame(pdf)

    def reindex(self):
        pdf = self._pdf.reset_index(drop=True)
        return DataFrame(pdf)

    # don't know about name
    def strip(self, columns):
        """Remove NAs from columns"""
        pdf = self._pdf.dropna(subset=columns)
        return DataFrame(pdf)

    def join(self, rhs, columns, how="left", suffixes=("_lhs", "_rhs")):
        if not isinstance(rhs, DataFrame):
            raise TypeError("rhs is not of type DataFrame")
        if not isinstance(columns, list):
            raise TypeError("columns is not of type list")
        lhs = self._pdf
        rhs = rhs.convert()
        pdf = pd.merge(lhs, rhs, on=columns, how=how, suffixes=suffixes)
        return pdf.convert()

    def extend(self, rows):
        top = self._pdf
        bottom = rows.convert()
        return pd.concat([top, bottom]).convert()

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

    # tally?

    # split columns

    # “Pivotting” which converts between long and wide forms. tidyr 1.0.0 introduces pivot_longer() and pivot_wider(), replacing the older spread() and gather() functions. See vignette("pivot") for more details.

class PandasDataFrame(pd.DataFrame):
    def convert(self):
        return DataFrame(self)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
