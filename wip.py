def aggregate(self, apropos, by=None):
    data = self._data.copy()
    if by:
        data = data.groupby(by)
    data = data.agg(**apropos).reset_index()
    return DataFrame(data)
