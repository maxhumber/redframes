<div align="center">
  <img alt="redframes" src="images/redframes.png" height="200px">
  <br/>
  <div align="center">
     <a href="https://pandas.pydata.org/"><img alt="Pandas Version" src="https://img.shields.io/badge/pandas-≥1.5,<2.0-blue"></a>  
    <a href="https://pypi.python.org/pypi/redframes"><img alt="PyPI" src="https://img.shields.io/pypi/v/redframes.svg"></a>
    <a href="https://pepy.tech/project/redframes"><img alt="Downloads" src="https://pepy.tech/badge/redframes"></a>
  </div>
  <br/>
</div>


### About

**redframes** (**re**ctangular **d**ata **frames**) is a general purpose data manipulation library that prioritizes syntax,  simplicity, and speed (to a solution). Importantly, the library is fully interoperable with [pandas](https://github.com/pandas-dev/pandas), compatible with [scikit-learn](https://github.com/scikit-learn/scikit-learn), and works great with [matplotlib](https://github.com/matplotlib/matplotlib). 



### Install & Import

```sh
pip install redframes
```

```python
import redframes as rf
```



### Quickstart

Copy-and-paste this to get started:

```python
import redframes as rf

df = rf.DataFrame({
    'bear': ['Brown bear', 'Polar bear', 'Asian black bear', 'American black bear', 'Sun bear', 'Sloth bear', 'Spectacled bear', 'Giant panda'],
    'genus': ['Ursus', 'Ursus', 'Ursus', 'Ursus', 'Helarctos', 'Melursus', 'Tremarctos', 'Ailuropoda'],
    'weight (male, lbs)': ['300-860', '880-1320', '220-440', '125-500', '60-150', '175-310', '220-340', '190-275'],
    'weight (female, lbs)': ['205-455', '330-550', '110-275', '90-300', '45-90', '120-210', '140-180', '155-220']
})

# | bear                | genus      | weight (male, lbs)   | weight (female, lbs)   |
# |:--------------------|:-----------|:---------------------|:-----------------------|
# | Brown bear          | Ursus      | 300-860              | 205-455                |
# | Polar bear          | Ursus      | 880-1320             | 330-550                |
# | Asian black bear    | Ursus      | 220-440              | 110-275                |
# | American black bear | Ursus      | 125-500              | 90-300                 |
# | Sun bear            | Helarctos  | 60-150               | 45-90                  |
# | Sloth bear          | Melursus   | 175-310              | 120-210                |
# | Spectacled bear     | Tremarctos | 220-340              | 140-180                |
# | Giant panda         | Ailuropoda | 190-275              | 155-220                |

(
    df
        .rename({"weight (male, lbs)": "male", "weight (female, lbs)": "female"})
        .gather(["male", "female"], into=("sex", "weight"))
        .split("weight", into=["min", "max"], sep="-")
        .gather(["min", "max"], into=("stat", "weight"))
        .mutate({"weight": lambda row: float(row["weight"])})
        .group(["genus", "sex"])
        .rollup({"weight": ("weight", rf.stat.mean)})
        .spread("sex", using="weight")
        .mutate({"dimorphism": lambda row: round(row["male"] / row["female"], 2)})
        .drop(["male", "female"])
        .sort("dimorphism", descending=True)
)

# | genus      |   dimorphism |
# |:-----------|-------------:|
# | Ursus      |         2.01 |
# | Tremarctos |         1.75 |
# | Helarctos  |         1.56 |
# | Melursus   |         1.47 |
# | Ailuropoda |         1.24 |
```



For comparison, here's the equivalent pandas:

```python
import pandas as pd

# df = pd.DataFrame({...})

df = df.rename(columns={"weight (male, lbs)": "male", "weight (female, lbs)": "female"})
df = pd.melt(df, id_vars=['bear', 'genus'], value_vars=['male', 'female'], var_name='sex', value_name='weight')
df[["min", "max"]] = df["weight"].str.split("-", expand=True)
df = df.drop("weight", axis=1)
df = pd.melt(df, id_vars=['bear', 'genus', 'sex'], value_vars=['min', 'max'], var_name='stat', value_name='weight')
df['weight'] = df["weight"].astype('float')
df = df.groupby(["genus", "sex"])["weight"].mean()
df = df.reset_index()
df = pd.pivot_table(df, index=['genus'], columns=['sex'], values='weight')
df = df.reset_index()
df = df.rename_axis(None, axis=1)
df["dimorphism"] = round(df["male"] / df["female"], 2)
df = df.drop(["female", "male"], axis=1)
df = df.sort_values("dimorphism", ascending=False)
df = df.reset_index(drop=True)

# 🤮
```



### IO

Save, load, and convert `rf.DataFrame` objects:

```python
# save .csv
rf.save(df, "bears.csv")

# load .csv
df = rf.load("bears.csv")

# convert redframes → pandas
pandas_df = rf.unwrap(df)

# convert pandas → redframes
df = rf.wrap(pandas_df)
```



### Verbs

Verbs are [pure](https://en.wikipedia.org/wiki/Pure_function) and "chain-able" methods that manipulate `rf.DataFrame` objects. Here is the complete list (see *docstrings* for examples and more details):

| Verb                                             | Description                                                  |
| ------------------------------------------------ | ------------------------------------------------------------ |
| `accumulate`<sup>‡</sup>                         | Run a cumulative sum over a column                           |
| `append`                                         | Append rows from another DataFrame                           |
| `combine`                                        | Combine multiple columns into a single column (opposite of `split`) |
| `cross`                                          | Cross join columns from another DataFrame                    |
| `dedupe`                                         | Remove duplicate rows                                        |
| [`denix`](https://www.dictionary.com/browse/nix) | Remove rows with missing values                              |
| `drop`                                           | Drop entire columns (opposite of `select`)                   |
| `fill`                                           | Fill missing values "down", "up", or with a constant         |
| `filter`                                         | Keep rows matching specific conditions                       |
| `gather`<sup>‡</sup>                             | Gather columns into rows (opposite of `spread`)              |
| `group`                                          | Prepare groups for compatible verbs<sup>‡</sup>              |
| `join`                                           | Join columns from another DataFrame                          |
| `mutate`                                         | Create a new, or overwrite an existing column                |
| `pack`<sup>‡</sup>                               | Collate and concatenate row values for a target column (opposite of `unpack`) |
| `rank`<sup>‡</sup>                               | Rank order values in a column                                |
| `rename`                                         | Rename column keys                                           |
| `replace`                                        | Replace matching values within columns                       |
| `rollup`<sup>‡</sup>                             | Apply summary functions and/or statistics to target columns  |
| `sample`                                         | Randomly sample any number of rows                           |
| `select`                                         | Select specific columns (opposite of `drop`)                 |
| `shuffle`                                        | Shuffle the order of all rows                                |
| `sort`                                           | Sort rows by specific columns                                |
| `split`                                          | Split a single column into multiple columns (opposite of `combine`) |
| `spread`                                         | Spread rows into columns (opposite of `gather`)              |
| `take`<sup>‡</sup>                               | Take any number of rows (from the top/bottom)                |
| `unpack`                                         | "Explode" concatenated row values into multiple rows (opposite of `pack`) |



### Properties

In addition to all of the verbs there are several properties attached to each `DataFrame` object:

```python
df["genus"] 
# ['Ursus', 'Ursus', 'Ursus', 'Ursus', 'Helarctos', 'Melursus', 'Tremarctos', 'Ailuropoda']

df.columns 
# ['bear', 'genus', 'weight (male, lbs)', 'weight (female, lbs)']

df.dimensions
# {'rows': 8, 'columns': 4}

df.empty
# False

df.memory
# '2 KB'

df.types
# {'bear': object, 'genus': object, 'weight (male, lbs)': object, 'weight (female, lbs)': object}
```



### matplotlib

`rf.DataFrame` objects integrate seamlessly with `matplotlib`:

```python
import redframes as rf
import matplotlib.pyplot as plt

football = rf.DataFrame({
    'position': ['TE', 'K', 'RB', 'WR', 'QB'],
    'avp': [116.98, 131.15, 180, 222.22, 272.91]
})

df = (
    football
        .mutate({"color": lambda row: row["position"] in ["WR", "RB"]})
        .replace({"color": {False: "orange", True: "red"}})
)

plt.barh(df["position"], df["avp"], color=df["color"]);
```

<img alt="redframes" src="images/bars.png" height="200px">



### scikit-learn

`rf.DataFrame` objects are fully compatible with `sklearn` functions, estimators, and transformers:

```python
import redframes as rf
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

df = rf.DataFrame({
    "touchdowns": [15, 19, 5, 7, 9, 10, 12, 22, 16, 10],
    "age": [21, 22, 21, 24, 26, 28, 30, 35, 28, 21],
    "mvp": [1, 1, 0, 0, 0, 0, 0, 1, 0, 0]
})

target = "touchdowns"
y = df[target]
X = df.drop(target)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=1)

model = LinearRegression()
model.fit(X_train, y_train)
model.score(X_test, y_test)
# 0.5083194901655527

print(X_train.take(1))
# rf.DataFrame({'age': [21], 'mvp': [0]})

X_new = rf.DataFrame({'age': [22], 'mvp': [1]})
model.predict(X_new)
# array([19.])
```
