import unittest

import pandas as pd

import redframes as rf
from redframes.verbs import append


class TestVerbAppend(unittest.TestCase):
    def test_no_side_effects(self):
        df1 = pd.DataFrame({"foo": [1, 2]})
        df2 = pd.DataFrame({"foo": [3, 4]})
        df1_start = df1.copy()
        df2_start = df2.copy()
        _ = append(df1, df2)
        self.assertTrue(df1_start.equals(df1))
        self.assertTrue(df2_start.equals(df2))

    def test_bad_type(self):
        df = rf.DataFrame({"foo": [1, 2, 3]})
        message = "df type is invalid, must be rf.DataFrame"
        with self.assertRaisesRegex(TypeError, message):
            df.append(1)

    def test_mismatched_columns(self):
        df1 = rf.DataFrame({"foo": [1, 2, 3]})
        df2 = rf.DataFrame({"bar": [4, 5, 6]})
        result = df1.append(df2)
        data = {"foo": [1, 2, 3, None, None, None], "bar": [None, None, None, 4, 5, 6]}
        expected = rf.DataFrame(data)
        self.assertEqual(result, expected)

    def test_matched_columns_single(self):
        df1 = rf.DataFrame({"foo": [1, 2, 3]})
        df2 = rf.DataFrame({"foo": [4, 5, 6]})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": [1, 2, 3, 4, 5, 6]})
        self.assertEqual(result, expected)

    def test_matched_columns_double(self):
        df1 = rf.DataFrame({"foo": [1, 2], "bar": ["a", "b"]})
        df2 = rf.DataFrame({"foo": [3, 4], "bar": ["c", "d"]})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": [1, 2, 3, 4], "bar": ["a", "b", "c", "d"]})
        self.assertEqual(result, expected)

    def test_matched_columns_double_out_of_order(self):
        df1 = rf.DataFrame({"foo": [1, 2], "bar": ["a", "b"]})
        df2 = rf.DataFrame({"bar": ["c", "d"], "foo": [3, 4]})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": [1, 2, 3, 4], "bar": ["a", "b", "c", "d"]})
        self.assertEqual(result, expected)
