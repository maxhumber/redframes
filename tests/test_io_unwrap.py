import unittest
from copy import deepcopy

import pandas as pd

import redframes as rf


class TestIOUnwrap(unittest.TestCase):
    def test_no_side_effect(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        df_start = deepcopy(df)
        pdf = rf.unwrap(df)
        pdf.columns = ["oof", "rab"]
        self.assertEqual(df, df_start)

    def test_simple(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        result = rf.unwrap(df)
        expected = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.assertTrue(result.equals(expected))
