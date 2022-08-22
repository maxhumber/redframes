import pandas as pd

def load(path, *args, **kwargs):
    pdf = pd.read_csv(path, *args, **kwargs)
    return DataFrame(pdf)


class DataFrame:
    def __repr__(self):
        # FIXME: not returning properly
        return self._pdf.to_string()

    def __init__(self, data):
        if isinstance(data, pd.DataFrame):
            self._pdf = data
        else:
            self._pdf = pd.DataFrame(data)

    def dump(path, *args, **kwargs):
        kwargs = {"index": False} | kwargs
        pdf = self._pdf
        pdf.to_csv(path, *args, **kwargs)
        return DataFrame(pdf)

    def select(self, columns):
        pdf = self._pdf[columns]
        return DataFrame(pdf)

    def rename(self, columns):
        pdf = self._pdf.rename(columns=columns)
        return DataFrame(pdf)

    def mutate(self, mutations):
        pdf = self._pdf.copy()
        for column, mutation in mutations.items():
            pdf[column] = pdf.apply(mutation, axis=1)
        return DataFrame(pdf)

    def filter(self, func):
        pdf = self._pdf.loc[func]
        return DataFrame(pdf)

    def take(self, rows, at="random"):
        # FIXME: turn into match and add warnings
        if at == "head":
            pdf = self._pdf.head(rows)
            return DataFrame(pdf)
        if at == "tail":
            pdf = self._pdf.tail(rows)
            return DataFrame(pdf)
        if at == "random":
            pdf = self._pdf.sample(rows)
            return DataFrame(pdf)

    def sort(self, columns, reverse=False):
        pdf = self._pdf.sort_values(by=columns, ascending=not reverse)
        return DataFrame(pdf)

    def reindex(self):
        pdf = self._pdf.reset_index(drop=True)
        return DataFrame(pdf)

    def strip(self, columns):
        """Remove NAs from columns"""
        pdf = self._pdf.dropna(subset=columns)
        return DataFrame(pdf)

    def dedupe(self, columns=None, keep="first"):
        pdf = self._pdf.drop_duplicates(subset=columns, keep=keep)
        return DataFrame(pdf)

    def join(self, rhs, columns, how="left", suffixes=("_lhs", "_rhs")):
        lhs = self._pdf
        rhs = rhs.convert()
        pdf = pd.merge(lhs, rhs, on=columns, how=how, suffixes=suffixes)
        return DataFrame(pdf)

    def extend(self, rows):
        top = self._pdf
        bottom = rows.convert()
        pdf = pd.concat([top, bottom])
        return DataFrame(pdf)

    def convert(self):
        return self._pdf

    # something about group by / summarise

# cast, recast, decast, backcast, open/close, in/out, up/down??
class PandasDataFrame(pd.DataFrame):
    def revert(self):
        return DataFrame(self)
