**WARNING: BETA, Under Active Development (v1 API 98% complete), writing all the docstrings, readme, + rest of the unit tests right now**

### redframes 

Name: [re]ctangular[d]ata[frames]

Logo: will be a panda with Red Sunglasses 🐼🟥🕶 😜



### Guiding Philosophy

- Less googling. 
- More Pythonic/Ergonomic feel. 

- Does 90% of what pandas can do (never will do 100%)
  - Drop down to pandas (unwrap) when needed
- Method chaining!
- Lambdas!!
- Everything returned from DataFrame is a standard python object
- Speed is not a priority ATM (but it shouldn't be super slow at <10M rows)
- No indexes (or confusing multi-index)
- No duplicate columns
- No side-effects (or changes in place)



### Install (beta):

```sh
pip install git+https://github.com/maxhumber/redframes.git
```



### Quickstart

```python
import redframes as rf

raw = rf.load("nfl.csv")

df = (
    raw
    .combine(["First Name", "Last Name"], into="name", sep=" ")
    .drop(["Rank", "ID"])
    .rename({
        "Position": "pos", 
        "Team": "team", 
        "Points": "points", 
        "Bye Week": "bye"
    })
    .select(["name", "pos", "team", "points", "bye"])
    .filter(lambda row: 
        (row["pos"].isin(["RB", "WR", "QB"])) &
        (row["points"] >= 100)
    )
    .sort(["team", "pos", "points"])
    .group(["team", "pos"])
    .take(1)
    .mutate({"pts_per_game": lambda row: round(row["points"] / 17, 1)})
    .drop("points")
    .group("team")
    .summarize({"mean_pts_per_game": ("pts_per_game", rf.stat.mean)})
    .sort("mean_pts_per_game", descending=True)
)

rf.unwrap(df)
```



### API:

<u>Import</u>:

```python
import redframes as rf
```



<u>IO (In-Out)</u>:

```python 
df = rf.load("path_to.csv") # load a csv file
rf.save(df, "path_to.csv") # save a df to a csv file
pdf = rf.wrap(df) # convert a rf.DataFrame to a pd.DataFrame
df = rf.unwrap(pdf) # convert a pd.DataFrame to a rf.DataFrame
```



<u>Init</u>:

```python
# (only) with dict
df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
```



<u>Properties & "Magics"</u>:

```python
df["foo"] # returns column as Python List of Values
df.columns # returns Python List of Columns
df.dimensions # returns {"rows": 10, "columns": 3}
df.empty # returns bool
df.types # returns {"foo": str, "bar": float}
```



<u>Verbs (see /docs/docs.ipynb for now...!)</u>: (Will properly document...)

```python
df.accumulate() # cumsum
df.append() # append rows (like pd.concat)
df.combine() # combine multiple columns into one (opposite of split)
df.dedupe() # drop duplicates (like df.drop_duplicates)
df.denix() # drop nil/null/na/none (like df.dropna)
df.drop() # drop select columns
df.fill() # fill up/down or with a constant (like df.fillna)
df.filter() # filter with a lambda function
df.gather() # gather (opposite of spread, like pd.melt)
df.group() # like df.groupby (works with: take, accumulate, rank, summarize)
df.join() # join two dataframes together (like pd.merge)
df.mutate() # mutate a new column (like df.apply)
df.rank() # rank column (dense)
df.rename() # rename columns
df.replace() # replace values in a column
df.sample() # sample a number of rows
df.select() # select columns
df.shuffle() # shuffle rows
df.sort() # sort rows
df.split() # split column (oppsite of combine)
df.spread() # spread columns (oppsite of gather, like pivot_table)
df.summarize() # summarise statistics (like agg)
df.take() # take any number of rows (like had, tail)
```



### Questions

- Did you encounter any bugs? Anything unexpected?
- Any verbs you would want renamed?
- Any other verbs I'm missing?
- Any missing properties/magics for DataFrame?
- Any super super unclear?
- .rename / Continue pandas format, or do `{"New": "Old"}`?
- Summarize: continue to use tuple? Or change to dict? `{new: {"old": stat}}`
- Should I totally get rid of index rows (in jupyter html output display)?
- Should `save` be a verb/method? Or keep in `io` namespace?
- Should I add a `log` method (that would print out for shape and column names at each step)?
- Please try with visualization!
- Please try with sklearn (I think it works, took a while to gerry-rig it)!!