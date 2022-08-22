import pandas as pd


def load(path, *args, **kwargs):
    pdf = pd.read_csv(path, *args, **kwargs)
    return DataFrame(pdf)


class DataFrame:
    # FIXME: not returning properly
    def __repr__(self):
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

    def head(self, n=5):
        pdf = self._pdf.head(n)
        return DataFrame(pdf)

    def tail(self, n=5):
        pdf = self._pdf.tail(n)
        return DataFrame(pdf)

    def sample(self, n=1):
        pdf = self._pdf.sample(n)
        return DataFrame(pdf)

    def sort(self, columns, reverse=False):
        ascending = False if reverse else True
        pdf = self._pdf.sort_values(by=columns, ascending=ascending)
        return DataFrame(pdf)

    def reindex(self):
        pdf = self._pdf.reset_index(drop=True)
        return DataFrame(pdf)

    # dropna -> winnow, sieve, cleanse, purge, strip, filtrate, discharge, refine
    # concentrate
    def strip(self, columns):
        pdf = self._pdf.dropna(subset=columns)
        return DataFrame(pdf)

    def dedupe(self, columns, keep="first"):
        pdf = self._pdf.drop_duplicates(subset=columns, keep=keep)
        return DataFrame(pdf)

    def convert(self):
        return self._pdf

    def join(self, rhs, columns, how="left", suffixes=("_lhs", "_rhs")):
        lhs = self._pdf
        rhs = rhs.convert()
        pdf = pd.merge(lhs, rhs, on=columns, how=how, suffixes=suffixes)
        return DataFrame(pdf)

    @property
    def dimensions(self):
        pass

    # do I want .info() ? .describe?
    # something about how many categories?

    # something about group by / summarise
    # append/extend
    # extend
    # unique
    # replace
    # fill
