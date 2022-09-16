### WIP (BETA)



[re]ctangular[d]ata[frames]



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



### Install

```sh
pip install git+https://github.com/maxhumber/redframes.git
```



### Quickstart

```python
import redframes as rf
import pandas as pd

pdf = pd.DataFrame([
    [12, "D'Marcus", "Williums", "East", "WR", 253.21],
    [14, "T.J.", "Juckson", "East", "WR", 239.99],
    [67, "T'Variuness", "King", "East", "WR", 173.46],
    [88, "Tyroil", "Smoochie-Wallace", "East", "QB", 367.15],
    [91, "D'Squarius", "Green, Jr.", "East", "TE", None],
    [3, "Ibrahim", "Moizoos", "East", "TE", 134.21],
    [13, "Jackmerius", "Tacktheratrix", "East", "RB", 228.42],
    [55, "D'Isiah T.", "Billings-Clyde", "East", "WR", 100],
    [0, "D'Jasper", "Probincrux III", "East", "RB", 180.14],
    [1, "Leoz Maxwell", "Jilliumz", "East", None, 170.21],
    [68, "Javaris Jamar", "Javarison-Lamar", "East", "WR", 87.29],
    [69, "Davoin", "Shower-Handel", None, "TE", 99.76],
    [77, "Hingle", "McCringleberry", None, "RB", 132.63],
    [89, "L'Carpetron", "Dookmarriot", "East", "QB", 240.5],
    [20, "J'Dinkalage", "Morgoone", "East", "K", 118.12],
    [17, "Xmus Jaxon", "Flaxon-Waxon", "East", "QB", 211.07],
    [10, "Saggitariutt", "Jefferspin", "West", "QB", 355.8],
    [11, "D'Glester", "Hardunkichud", "West", "WR", 305.45],
    [91, "Swirvithan", "L'Goodling-Splatt", "West", "WR", 147.47],
    [44, "Quatro", "Quatro", "West", "WR", 98.29],
    [19, "Ozamataz", "Buckshank", "West", "RB", 85.58],
    [12, "Beezer Twelve", "Washingbeard", "West", "RB", None],
    [55, "Shakiraquan T.G.I.F.", "Carter", "West", "TE", 148.33],
    [70, "X-Wing", "@Aliciousness", "West", "RB", 12.00],
    [36, "Sequester", "Grundelplith M.D.", "West", "WR", 228.26],
    [4, "Scoish Velociraptor", "Maloish", "West", "TE", None],
    [5, "T.J. A.J. R.J.", "Backslashinfourth V", "West", "RB", 183.12],
    [33, "Eeee", "Eeeeeeeee", "West", "QB/RB", 200.01],
    [88, "Donkey", "Teeth", "West", "TE", 56.2],
    [88, "Donkey", "Teeth", "West", "TE", 56.2],
    [88, "Donkey", "Teeth", "West", "TE", 56.2],
    [15, "Torque (Construction Drilling Noise)", "Lewith", None, "K", 153.70],
    [6, "(The Player", "Formerly Known As Mousecop)", None, "K", 121.65],
    [2, "Dan", "Smith", "West", "QB", 367.69]
], columns=["Number", "First Name", "Last Name", "Team", "Position", "Points"])

# wrap pd.DataFrame into a rf.DataFrame
rdf = rf.wrap(pdf)

# chain method/verbs together
df = (
    rdf
    .combine(["First Name", "Last Name"], into="name", sep=" ")
    .rename({
        "Position": "position", 
        "Team": "team", 
        "Points": "points", 
    })
    .select(["name", "team", "position", "points"])
    .dedupe("name")
    .fill("team", direction="down")
    .denix(["position", "points"])
    .group("position")
    .rank("points", into="rank", descending=True)
    .filter(lambda row: 
        (row["position"].isin(["RB", "WR", "QB"])) &
        (row["points"] >= 100)
    )
    .sort(["position", "points"], descending=True)
    .mutate({"pts_per_game": lambda row: round(row["points"] / 17, 1)})
    .drop("points")
    .group("team")
    .summarize({"mean_ppg": ("pts_per_game", rf.stat.mean)})
)
```



### API

<u>Import</u>:

```python
import redframes as rf
```



<u>IO (In-Out)</u>:

```python 
df = rf.load("path_to.csv") # load a csv file
rf.save(df, "path_to.csv") # save a df to a csv file
df = rf.wrap(pdf) # convert a pd.DataFrame to a rf.DataFrame
pdf = rf.unwrap(df) # convert a rf.DataFrame to a pd.DataFrame
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