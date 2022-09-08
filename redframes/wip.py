import itertools
import pandas as pd

class DataFrame:
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

    def dump(self, path=None):
        data = self._data.copy()
        if not path:
            return data.to_dict(orient="list")
        if not path.endswith(".csv"):
            raise TypeError
        kwargs = {"index": False} | kwargs
        data.to_csv(path)
        return DataFrame(data)
