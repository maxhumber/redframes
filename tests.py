import pandas as pd
import bearcats as bc

pd.set_option('display.float_format', lambda x: '%.2f' % x)


bc.load()

df_a = pd.DataFrame({
    "team": ["leafs", "rangers", "kings", "blackhawks"],
    "salary": [98, 87, 88, 84]
})


#
# bc.DataFrame({
#     "team"
# })
#
# bc.load("")

df_b = pd.DataFrame({
    "team": ["leafs", "rangers", "blackhawks", "senators"],
    "percent": [0.7, 0.6, 0.65, 0.30]
})

bf_a = DataFrame(df_a)
bf_b = DataFrame(df_b)

bf_a\
    .join(bf_b, ["team"])\
    .strip(["percent"])\
    .reindex()

# clean


df = pd.DataFrame({
    "a": [1, 2, 3, 4, 5],
    "b": ["v", "w", "x", "y", "z"],
    "c": ["dog", "cat", "giraffe", "eel", "snake"],
    "d": [1.0, 3.99, 4.88, 1_000_300.19, 0.2222]
})



bf = DataFrame(df)

bf.


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


bf
