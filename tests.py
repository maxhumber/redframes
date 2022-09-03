import pandas as pd
import bearcats as bc

pd.set_option("display.float_format", lambda x: "%.2f" % x)


df = pd.DataFrame({
    "country": ["Can", "Can", "Can", "Can", "Can"],
    "geo": ["ON", "ON", "ON", "NFLD", "NFLD"],
    "stat": ["pop", "gdp", "temp", "pop", "temp"],
    "value": [16, 50, 31, 1, 24.5]
})

df


# SPREAD

def pivot_wider(df, names_from, values_from):
    index = [col for col in df.columns if col not in [names_from, values_from]]
    df = pd.pivot_table(df, index=index, columns=[names_from], values=[values_from])
    df.columns = [col for col in df.columns.get_level_values(1)]
    df = df.reset_index().rename_axis(None, axis=0)
    return df

pivot_wider(df, names_from="stat", values_from="value")

def spread(df, column, over):
    index = [col for col in df.columns if col not in [column, over]]
    df = pd.pivot_table(df, index=index, columns=[column], values=[over])
    df.columns = [col for col in df.columns.get_level_values(1)]
    df = df.reset_index().rename_axis(None, axis=0)
    return df

df = spread(df, "stat", "value")
df

# GATHER

def gather(df, columns, into=("variable", "value"), dropna=True):
    index = [col for col in df.columns if col not in columns]
    df = pd.melt(df, id_vars=index, value_vars=columns, var_name=into[0], value_name=into[1])
    df = df.dropna(subset="value")
    return df

gather(df, ["pop", "gdp", "temp"])


df = (
    pd.melt(df, id_vars=index, value_vars=['gdp', "pop", "temp"], var_name='variable', value_name='value')
        .dropna(subset="value")
)





###

index = "geo"
df = pd.pivot_table(df, index=[index], columns=['stat'], values=["value"])
df.columns = [col for col in df.columns.get_level_values(1)]
df = df.rename_axis(None, axis=0).reset_index()
df = df.rename(columns={"index": "geo"})


df = pd.pivot_table(df, index=['geo'], columns=['stat'], values=["value"])
df.columns = [col for col in df.columns.get_level_values(1)]
df = df.rename_axis(None, axis=0).reset_index()
df


df.columns = [' '.join(col).strip() for col in df.columns.values]
df




df = pd.DataFrame({
    "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
    "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
    "C": ["small", "large", "large", "small", "small", "large", "small", "small", "large"],
    "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
    "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]
})

df

pd.pivot_table(df, values='D', index=['A'], columns=['C'], aggfunc="sum")


df = pd.DataFrame({
    "continent": ["NA", "NA", "NA", "EUR", "EUR", "ASIA", "ASIA", "ASIA"],
    "country": ["CAN", "MEX", "USA", "GBP", "FRA", "JAP", "CHN", "KOR"],
    "gdp": [60, 30, 390, 90, 85, 88, 360, 45]
})


list(map(lambda x: x * 5, [1, 2, 3]))


df = (
    rf.load("example.csv")
    .select(["a", "d"])
    .map({
        "e": lambda d: d["d"] * 5,
        "f": lambda d: d["e"] / 5
    })
    .filter(lambda d: d["a"] >= 3)
    .head(5)
    .sort(["d"], reverse=True)
    .rename({"f": "h"})
    .dump("")
)

df.types["a"]
df.empty
df.dimensions
df.columns
df.values
df["e"]



df = bc.DataFrame({
    "continent": ["NA", "NA", "NA", "EUR", "EUR", "ASIA", "ASIA", "ASIA"],
    "country": ["CAN", "MEX", "USA", "GBP", "FRA", "JAP", "CHN", "KOR"],
    "gdp": [60, 30, 390, 90, 85, 88, 360, 45]
})

fancy_min = lambda x: min(x)
fancy_min([3, 4, 5, 2, 1])


(
    df
    .mutate({
        "gdp": lambda d: d["column"] * 1_000_000_000
    })
    .group(["continent"])
    .summarize({
        "total_gdp": ("gdp", bc.reducers.mean),
        "count_countries": (count, "gdp"),
        "fancy_gdp": min
    })
    .aggregate([
        ("count", "gdp", count),
        ("mean_gdp", "gdp", sum),
    ])
    .ungroup()
    .summarise([
        bc.summarise.sum
        ("gdp", sum),
        (sum, "gdp"),
        # mean_gdp = sum("gdp")
        ("mean_gdp", sum, "gdp")
    ])
    .summarise({
        ("sum_gdp", "gpd"):
        "sum": ("gdp", sum),
        "mean": ("gdp", mean),
        "new_func": ("gdp", lambda c: min(c))
    })
)

from functools import reduce
reduce(sum, [1, 2, 3])

DataFrame({"continent": [], "sum": []})

sum()
count()
median()
quantile([0.25,0.75])
apply(function)
min()
max()
mean()
var()
std()

df.reduce({
    "new_column_name": lambda x: x["column"].mean(),
    "new_column_name2": ("column", lambda c: c.mean()),
})



df.reduce(["continent"], {"gdp": [sum, lambda x: x.min()]})


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
        "d": [1, 3.99, None, 1_000_300.19, 0.2222],
    }
)

y = df["a"]
X = df[["b", "c", "d"]]

y.values
X.values
df.values.tolist()

pd.to_numeric()

pd.to_datetime("2018-01-01")

pd.to_numeric(df["d"])

help(df.astype)

bf = (
    bc.load("example.csv")
    .select(["a", "d"])
    .mutate({"e": lambda d: d["d"] * 5, "f": lambda d: d["e"] / 5})
    .filter(lambda d: d["a"] >= 3)
    .take(5, at="head")
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
