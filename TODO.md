### todo


- tests/
- ensure no side effects




- dump/save/export/write

### groupby

```
def aggregate(self, apropos, by=None):
    data = self._data.copy()
    if by:
        data = data.groupby(by)
    data = data.agg(**apropos).reset_index()
    return DataFrame(data)
```
unique values
value counts
tally?

what about common rowwise operators?
what about common groupby operators?
groupby dataframe type


----

- docstrings
- add datasets
- make sure it works with fantasy
- machine learning
- visualization
- cheat sheet
- 10 minutes
- readme

to_pandas
from_pandas
convert

SHOULD I INSTANTIATE OFF PANDAS? CONVERT FUNCTION OUTSIDE?
