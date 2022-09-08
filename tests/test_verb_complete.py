import unittest

import pandas as pd

import redframes as rf
from redframes.verbs import complete


class TestVerbComplete(unittest.TestCase):
    def test_no_side_effects(self):
        df = pd.DataFrame({"foo": ["a", "b", "b"], "bar": [1, 1, 2], "baz": [3, 4, 3]})
        df_start = df.copy()
        _ = complete(df, ["foo", "bar"])
        self.assertTrue(df_start.equals(df))
