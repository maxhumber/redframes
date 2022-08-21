import pandas as pd
# from bearcats import DataFrame

# get rid of all the return `DataFrame`s

class DataFrame:
    def __repr__(self):
        return str(self._pdf)

    def __init__(self, pdf):
        # should this be more exposed or less?
        self._pdf = pdf

    # mimics json library verb
    @classmethod
    def load(cls, path, *args, **kwargs):
        pdf = pd.read_csv(path, *args, **kwargs)
        return DataFrame(pdf)

    # mimics json library verb
    def dump(path, index=False, *args, **kwargs):
        # merge kwargs + index? and allow for overwrite
        pdf = self._pdf
        pdf.to_csv(path, *args, **kwargs)
        return DataFrame(pdf)

    # columns
    def select(self, columns):
        pdf = self._pdf[columns]
        return DataFrame(pdf)

    # rename
    def rename(self, columns):
        pdf = self._pdf.rename(columns=columns)
        return DataFrame(pdf)

    # rows
    # keep?
    def filter(self, func):
        pdf = self._pdf.loc[func]
        return DataFrame(pdf)

    # columns
    def mutate(self, mutations):
        pdf = self._pdf.copy()
        for column, mutation in mutations.items():
            pdf[column] = pdf.apply(mutation, axis=1)
        return DataFrame(pdf)

    # rows
    # top?
    def head(self, n=5):
        pdf = self._pdf.head(n)
        return DataFrame(pdf)

    # last
    def tail(self, n=5):
        pdf = self._pdf.tail(n)
        return DataFrame(pdf)

    def sample(self, n=1):
        # if fraction then do sample frac
        pdf = self._pdf.sample(n)
        return DataFrame(pdf)

    def sort(self, columns, reverse=False):
        pdf = self._pdf.sort_values(by=columns, ascending=False if reverse else True)
        return DataFrame(pdf)

    def drop(self):
        # NAs
        pass

    def join(self):
        # left, inner and on keys?
        # .left_join()
        # .inner_join()
        pass

    # what about concatenate? and (appending rows?)
    # .append() ?
    # .extend() ?

    @property
    def dimensions(self):
        pass

    # do I want .info() ? .describe?
    # something about how many categories?

    # something about group by / summarise


df = pd.DataFrame({
    "a": [1, 2, 3, 4, 5],
    "b": ["v", "w", "x", "y", "z"],
    "c": ["dog", "cat", "giraffe", "eel", "snake"],
    "d": [1.0, 3.99, 4.88, 1_000_300.19, 0.2222]
})

# bf = DataFrame(df)

bc.load()
bf.dump()

bf = (
    # call this a DataTable? Panel? Frame? Table? DF? BFrame??
    # should this be bc.loads()?
    # bc.DataFrame
    DataFrame.load("example.csv")
        .select(["a", "d"])
        # do I want this? adding complexity or simplicity?
        # .mutate(f=lambda d: d["e"] / 5)
        .mutate({
            "e": lambda d: d["d"] * 5,
            "f": lambda d: d["e"] / 5
        })
        .filter(lambda d: d["a"] >= 3)
        .head(5)
        .sort(["d"], reverse=True)
        .rename({"f": "h"})
        # .sort("d")
        # .select("f")
)

bf = (
    DataFrame.load("example.csv")
        .select(["a", "d"])
        .mutate({
            "e": lambda d: d["d"] * 5,
            "f": lambda d: d["e"] / 5
        })
        .filter(lambda d: d["a"] >= 3)
        .sort(["d"], reverse=True)
        .rename({"f": "h"})
        .head(5)
        # .convert() # to pandas
        # .as_pandas_dataframe()
        # .to_df()
        # .as_df()
        # .pdf
        # .unwrap()
        # .collect()
)

# what to do about these options?
# where should they go?
pd.set_option('display.float_format', lambda x: '%.2f' % x)

bf
