import unittest

import pandas as pd

import redframes as rf
from redframes.verbs import combine


class TestVerbCombine(unittest.TestCase):
    def test_no_side_effects(self):
        df = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        df_start = df.copy()
        _ = combine(df, ["foo", "bar"], "-", into="baz")
        self.assertTrue(df_start.equals(df))

    def test_columns_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "columns type is invalid, must be list"):
            df.combine("foo", sep="-", into="foo")

    def test_sep_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "sep type is invalid, must be str"):
            df.combine(["foo", "bar"], sep=1, into="baz")

    def test_into_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "into type is invalid, must be str"):
            df.combine(["foo", "bar"], sep="_", into=["baz"])

    def test_two_columns(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        result = df.combine(["foo", "bar"], sep="_", into="baz")
        expected = rf.DataFrame({"baz": ["1_4", "2_5", "3_6"]})
        self.assertEqual(result, expected)

    def test_two_columns_overwrite(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        result = df.combine(["foo", "bar"], sep="_", into="foo")
        expected = rf.DataFrame({"foo": ["1_4", "2_5", "3_6"]})
        self.assertEqual(result, expected)

    def test_three_columns(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
        result = df.combine(["foo", "bar", "baz"], sep=":", into="all")
        expected = rf.DataFrame({"all": ["1:3:5", "2:4:6"]})
        self.assertEqual(result, expected)
