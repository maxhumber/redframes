<div align="center">
  <img alt="redframes" src="images/logo.png" height="200px">
  <br/>
  <div align="center">
    <a href="https://pypi.python.org/pypi/redframes"><img alt="PyPI" src="https://img.shields.io/pypi/v/redframes.svg"></a>
    <a href="https://pandas.pydata.org/"><img alt="Pandas Version" src="https://img.shields.io/badge/pandas-1.5%2B-blue"></a>  
    <a href="https://pepy.tech/project/redframes"><img alt="Downloads" src="https://pepy.tech/badge/redframes"></a>
  </div>
  <br/>
</div>

**redframes** (**re**ctangular **d**ata**frames**) is a data manipulation library for "medium" data. 



It is fully interoperable with [pandas](https://github.com/pandas-dev/pandas), compatible with [scikit-learn](https://github.com/scikit-learn/scikit-learn), and works great with [matplotlib](https://github.com/matplotlib/matplotlib)!

<b style="color:red;">red</b><b>frames</b> prioritizes syntax over flexibility and scope. And minimizes the *number-of-googles-per-lines-of-code*™ so that you can focus on the work that matters most.



### Install & Import

```sh
pip install redframes
```

```python
import redframes as rf
```



### Quickstart

Copy-and-paste this:

```python
import redframes as rf

df = rf.DataFrame({
    "foo": ["A", "A", "B", None, "B", "A", "A", "C"],
    "bar": [1, 4, 2, -4, 5, 6, 6, -2], 
    "baz": [0.99, None, 0.25, 0.75, 0.66, 0.47, 0.48, None]
})

# | foo   |   bar |    baz |
# |:------|------:|-------:|
# | A     |     1 |   0.99 |
# | A     |     4 |        |
# | B     |     2 |   0.25 |
# |       |    -4 |   0.75 |
# | B     |     5 |   0.66 |
# | A     |     6 |   0.47 |
# | A     |     6 |   0.48 |
# | C     |    -2 |        |

(
    df
    .mutate({"bar100": lambda row: row["bar"] * 100})
    .select(["foo", "baz", "bar100"])
    .filter(lambda row: (row["foo"].isin(["A", "B"])) & (row["bar100"] > 0))
    .denix("baz")
    .group("foo")
    .rollup({
        "bar_mean": ("bar100", rf.stat.mean), 
        "baz_sum": ("baz", rf.stat.sum)
    })
    .gather(["bar_mean", "baz_sum"], into=("variable", "value"))
    .sort("value")
)

# | foo   | variable   |   value |
# |:------|:-----------|--------:|
# | B     | baz_sum    |   0.91  |
# | A     | baz_sum    |   1.94  |
# | B     | bar_mean   | 350     |
# | A     | bar_mean   | 433.333 |
```



### IO

|               |      |
| ------------- | ---- |
| `rf.load()`   |      |
| `rf.save()`   |      |
| `rf.wrap()`   |      |
| `rf.unwrap()` |      |



```python
import redframes as rf
import pandas as pd

df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})

# save/load
rf.save(df, "example.csv")
df = rf.load("example.csv")

# to/from pandas
pandf = rf.unwrap(df)
reddf = rf.wrap(pandf)
```



### Verbs

|                                                  | Description |
| ------------------------------------------------ | ----------- |
| `accumulate`                                     |             |
| `append`                                         |             |
| `combine`                                        |             |
| `cross`                                          |             |
| `dedupe`                                         |             |
| [`denix`](https://www.dictionary.com/browse/nix) |             |
| `drop`                                           |             |
| `fill`                                           |             |
| `filter`                                         |             |
| `gather`                                         |             |
| `group`                                          |             |
| `join`                                           |             |
| `mutate`                                         |             |
| `pack`                                           |             |
| `rank`                                           |             |
| `rename`                                         |             |
| `replace`                                        |             |
| `rollup`                                         |             |
| `sample`                                         |             |
| `select`                                         |             |
| `shuffle`                                        |             |
| `sort`                                           |             |
| `split`                                          |             |
| `spread`                                         |             |
| `take`                                           |             |
| `unpack`                                         |             |



### Properties

|            |      |
| ---------- | ---- |
| ["column"] |      |
| columns    |      |
| dimensions |      |
| empty      |      |
| memory     |      |
| types      |      |
