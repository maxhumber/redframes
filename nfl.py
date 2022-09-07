import redframes as rf
import numpy as np
import pandas as pd

df = pd.DataFrame({
    "foo": ["a", "a", "a", "b", "b", "a"],
    "bar": [3, 2, 5, 6, 7, 1]
})

df.groupby(by=["foo"])["bar"].apply(lambda x: np.array(x).argsort())


df.groupby(by=["foo"]).agg(**{
    "cumsum": ("bar", np.cumsum),
    "rank": ("bar", lambda x: np.array(x).argsort())
})

df["rank"] = df.groupby(by=["foo"]).agg(**{""})
df


df.groupby(["foo"]).head(2)

array = np.array([10, 6, 3, 11, 1])
np.cumsum(array)
array.argsort()

rank = lambda x: np.array(x).argsort().argsort()
rank([4, 4, 0, 1])


df = rf.DataFrame("nfl.csv")
df = pd.read_csv("nfl.csv")
df.groupby("Position")


df.unwrap()
df.wrap()

.head(10)

(
    df.mutate({
        "name": lambda row: f"{row['First Name']} {row['Last Name']}"
    })
    .select([
        'name',
        'Position',
        'Team',
        'Points',
        'Bye Week'
    ])
    .group
    .aggregate({"avg_points": ("Points", np.mean)}, by=["Position"])
)
