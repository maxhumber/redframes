import pandas as pd
import bearcats as bc

pd.set_option("display.float_format", lambda x: "%.2f" % x)

df = bc.PandasDataFrame({
    "hi": [1, 2, 3]
})

isinstance(df, pd.DataFrame)

(
    bc.DataFrame({"a": range(100)})
    .take(10, at="head")
    .reindex()
)


df = bc.DataFrame({
    "a": [1, 1, None, 1, 2, None],
    "b": [0, 0, None, None, None, None]
})

df.mutate({
    "a": lambda d: str(d["a"]).replace("nan", "0")
}).strip(["b"])

dfa = bc.DataFrame({
    "team": ["leafs", "rangers", "kings", "blackhawks"],
    "salary": [98, 87, 88, 84]
})

dfb = bc.DataFrame({
    "team": ["leafs", "rangers", "blackhawks", "senators"],
    "percent": [0.7, 0.6, 0.65, 0.30],
})

dfc = (dfa
    .join(dfb, columns=["team"])
    .strip(columns=["percent"])
    .reindex()
    .pander()
)

type(bfa)

bfc


df = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5],
        "b": ["v", "w", "x", "y", "z"],
        "c": ["dog", "cat", "giraffe", "eel", "snake"],
        "d": [1.0, 3.99, 4.88, 1_000_300.19, 0.2222],
    }
)


bf = (
    bc.load("example.csv")
    .select(["a", "d"])
    .mutate({"e": lambda d: d["d"] * 5, "f": lambda d: d["e"] / 5})
    .filter(lambda d: d["a"] >= 3)
    .head(5)
    .sort(["d"], reverse=True)
    .rename({"f": "h"})
    # .sort("d")
    # .select("f")
)

df = (
    bc.load("example.csv")
    .select(["a", "d"])
    .mutate({
        "e": lambda d: d["d"] * 5,
        "f": lambda d: d["e"] / 5
    })
    .filter(lambda d: d["a"] >= 3)
    .sort(["d"], reverse=True)
    .rename({"f": "h"})
    .head(5)
)
