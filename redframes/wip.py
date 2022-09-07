from __future__ import annotations

import itertools
from typing import Any
from uuid import uuid4

import pandas as pd


class DataFrame:
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
        data = pd.melt(
            data,
            id_vars=index,
            value_vars=columns,
            var_name=into[0],
            value_name=into[1],
        )
        data = data.dropna(subset="value")
        return DataFrame(data)

    def complete(self, columns):
        data = self._data.copy()
        series = [data[column] for column in columns]
        index = pd.MultiIndex.from_tuples(itertools.product(*series), names=columns)
        data = data.set_index(columns)
        data = data.reindex(index)
        data = data.reset_index()
        return DataFrame(data)

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
