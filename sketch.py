import pandas as pd

class DataFrame:
    def __init__(self, pdf):
        self.pdf = pdf

    def __repr__(self):
        return str(self.pdf)
        # return self.pdf.to_string()

    def select(self, columns):
        pdf = self.pdf[columns]
        return DataFrame(pdf)

    @staticmethod
    def csv():
        pass

    @property
    def dimensions(self):
        pass

    def filter(self, func):
        pdf = self.pdf.loc[func]
        return DataFrame(pdf)

    def mutate(self, mutations):
        pdf = self.pdf.copy()
        for name, mutation in mutations.items():
            pdf[name] = pdf.apply(mutation, axis=1)
        return DataFrame(pdf)

    def head(self, n=5):
        pdf = self.pdf.head(n)
        return DataFrame(pdf)

    def tail(self, n=5):
        pdf = self.pdf.tail(n)
        return DataFrame(pdf)

    def sample(self, n=1):
        pdf = self.pdf.sample(n)
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

bf = DataFrame(df)

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
