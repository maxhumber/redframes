### todo

- aggregate
- Write more tests
- Ensure every verb has no ensure no side effects
- Ensure every verb creates/returns a valid df (index, columns)
- .log method? (with print out like Untitled12)
- docstrings
- make sure it works in machine learning pipeline
- make sure it works in visualization
- cheat sheet
- 10 minutes
- readme
- add datasets


```
def aggregate(self, apropos, by=None):
    data = self._data.copy()
    if by:
        data = data.groupby(by)
    data = data.agg(**apropos).reset_index()
    return DataFrame(data)
```
