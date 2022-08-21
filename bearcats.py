import pandas as pd
# from bearcats import DataFrame

class DataFrame:
    def __init__(self, pdf):
        self._pdf = pdf

    @property
    def bdf(self):
        pdf = self._pdf
        return DataFrame(pdf)

    @classmethod
    def load(cls, path, *args, **kwargs):
        pdf = pd.read_csv(path, *args, **kwargs)
        return DataFrame(pdf)

    def dump(path, index=False, *args, **kwargs):
        pdf = self._pdf
        self._pdf.to_csv(path, *args, **kwargs)
        return DataFrame(pdf)

    def __repr__(self):
        return str(self._pdf)

    def select(self, columns):
        pdf = self._pdf[columns]
        return DataFrame(pdf)

    @property
    def dimensions(self):
        pass

    def filter(self, func):
        pdf = self._pdf.loc[func]
        return DataFrame(pdf)

    def mutate(self, mutations):
        pdf = self._pdf.copy()
        for name, mutation in mutations.items():
            pdf[name] = pdf.apply(mutation, axis=1)
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

    def sort(self):
        pass

    def drop(self):
        pass

    def join(self):
        pass

df = pd.DataFrame({
    "a": [1, 2, 3, 4, 5],
    "b": ["v", "w", "x", "y", "z"],
    "c": ["dog", "cat", "giraffe", "eel", "snake"],
    "d": [1.0, 3.99, 4.88, 1_000_300.19, 0.2222]
})

# df.to_csv("example.csv")

DataFrame.load("example.csv")

pd.read_csv("example.csv", header=0)

bf = DataFrame(df)

# bf = DataFrame.import("")
# bf.export("example.csv")

# bf = DataFrame.from("example.csv")
# bf.to("example.csv")

# bf.load("example.csv")
# bf.dump("example.csv")

(bf
    .select(["a", "d"])
    .head(5)
    .mutate({
        "e": lambda d: d["d"] * 5
    })
    .filter(lambda d: d["a"] >= 3)
)


bf = (
    bf
    .select(["a", "b", "d"])
    .head(5)
    .mutate({
        "e": lambda x: x["d"] * 5,
        "f": lambda x: x["e"] / 5
    })
    .filter(lambda x: x["a"] >= 3)
)

#
