from multiprocessing.sharedctypes import Value
from sqlite3 import DatabaseError
import unittest
from pathlib import Path
from shutil import rmtree as delete
from tempfile import mkdtemp as make_temp_dir

import pandas as pd

import redframes as rf


class TestDataFrame(unittest.TestCase):
    def setUp(self):
        self.tempdir = make_temp_dir()

    def tearDown(self):
        delete(self.tempdir)

    def test_init_good_csv(self):
        path = str(Path(self.tempdir) / "test_init_good.csv")
        pd.DataFrame({"foo": [1, 2, 3], "bar": ["a", "b", "c"]}).to_csv(
            path, index=False
        )
        df = rf.DataFrame(path)
        self.assertIsNotNone(df)

    def test_init_bad_csv(self):
        with self.assertRaises(FileNotFoundError):
            rf.DataFrame("test_init_bad.csv")

    def test_init_bad_format(self):
        with self.assertRaises(TypeError):
            rf.DataFrame("test_init_bad_format.json")

    def test_init_good_dict(self):
        df = rf.DataFrame({"foo": [1, 2, 3]})
        self.assertIsNotNone(df)

    def test_init_bad_dict_values_not_in_list(self):
        with self.assertRaisesRegex(
            ValueError, "If using all scalar values, you must pass an index"
        ):
            rf.DataFrame({"foo": 0})

    def test_init_bad_dict_mismatched_lengths(self):
        with self.assertRaisesRegex(
            ValueError, "All arrays must be of the same length"
        ):
            rf.DataFrame({"foo": [0], "bar": [1, 2]})

    def test_init_pandas_dataframe(self):
        pdf = pd.DataFrame({"foo": [1, 2, 3]})
        rdf = rf.DataFrame(pdf)
        self.assertIsNotNone(rdf)

    def test_init_bad_type(self):
        with self.assertRaisesRegex(TypeError, r"Invalid data input type *"):
            rf.DataFrame(1)

    def test_repr(self):
        df = rf.DataFrame({"foo": [0]})
        result = repr(df)
        expected = "   foo\n0    0"
        self.assertEqual(result, expected)

    def test_repr_html(self):
        df = rf.DataFrame({"foo": [0, 1, 2]})
        result = df._repr_html_()[:50]
        expected = "<div>\n<style scoped>\n    .dataframe tbody tr th:on"
        self.assertEqual(result, expected)

    def test_getitem(self):
        df = rf.DataFrame({"foo": [1, 2, 3, 4], "bar": ["a", "b", "c", "d"]})
        result = df["bar"]
        expected = ["a", "b", "c", "d"]
        self.assertEqual(result, expected)

    def test_getitem_for_nonexistent_column(self):
        df = rf.DataFrame({"foo": [0]})
        with self.assertRaisesRegex(KeyError, "bar"):
            df["bar"]

    def test_eq(self):
        df1 = rf.DataFrame({"foo": [0]})
        df2 = rf.DataFrame({"foo": [0]})
        self.assertEqual(df1, df2)

    def test_eq_bad_df(self):
        df1 = rf.DataFrame({"bar": [0]})
        df2 = rf.DataFrame({"foo": [0]})
        self.assertNotEqual(df1, df2)

    def test_eq_bad_type(self):
        df = rf.DataFrame({"bar": [0]})
        with self.assertRaises(NotImplementedError):
            df == 0

    def test_shape(self):
        df = rf.DataFrame({"foo": range(3), "bar": ["a", "b", "c"]})
        self.assertEqual(df.shape, {"rows": 3, "columns": 2})

    def test_types(self):
        df = rf.DataFrame(
            {"foo": ["a", "b", "c"], "bar": [1, 2, 3], "baz": [4.0, None, 5.6]}
        )
        expected = {"foo": str, "bar": int, "baz": float}
        result = df.types
        self.assertEqual(result, expected)

    def test_columns(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        result = df.columns
        expected = ["foo", "bar"]
        self.assertEqual(result, expected)

    def test_rows(self):
        df = rf.DataFrame({"a": range(5), "b": range(5, 10), "c": range(10, 15)})
        result = df.rows
        expected = [[0, 5, 10], [1, 6, 11], [2, 7, 12], [3, 8, 13], [4, 9, 14]]
        self.assertEqual(result, expected)

    def test_empty_true(self):
        result = rf.DataFrame().empty
        self.assertTrue(result)

    def test_empty_false(self):
        result = rf.DataFrame({"foo": [0]}).empty
        self.assertFalse(result)

    def test_take(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.take()
        expected = rf.DataFrame({"foo": [0]})
        self.assertEqual(result, expected)

    def test_take_not_int(self):
        df = rf.DataFrame({"foo": range(100)})
        with self.assertRaisesRegex(TypeError, "Invalid rows argument *"):
            df.take("a")

    def test_take_zero(self):
        df = rf.DataFrame({"foo": range(100)})
        with self.assertRaisesRegex(ValueError, "Rows argument must not be 0"):
            df.take(0)

    def test_take_multiple(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.take(5)
        expected = rf.DataFrame({"foo": [0, 1, 2, 3, 4]})
        self.assertEqual(result, expected)

    def test_take_tail(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.take(-2)
        expected = rf.DataFrame({"foo": [98, 99]})
        self.assertEqual(result, expected)

    def test_take_over(self):
        df = rf.DataFrame({"foo": range(100)})
        with self.assertRaisesRegex(
            ValueError, "Rows argument exceeds total number of rows"
        ):
            df.take(101)

    def test_slice(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.slice(4, 6)
        expected = rf.DataFrame({"foo": [4, 5]})
        self.assertEqual(result, expected)

    def test_slice_negative(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.slice(90, -5)
        expected = rf.DataFrame({"foo": [90, 91, 92, 93, 94]})
        self.assertEqual(result, expected)

    def test_slice_bad_type(self):
        df = rf.DataFrame({"foo": range(100)})
        with self.assertRaises(TypeError):
            df.slice("a", 3)

    def test_sample_default(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.sample().shape["rows"]
        expected = 1
        self.assertEqual(result, expected)

    def test_sample_bad_rows_type(self):
        df = rf.DataFrame({"foo": range(100)})
        with self.assertRaisesRegex(TypeError, "Invalid rows argument *"):
            df.sample("a")

    def test_sample_one(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.sample(1).shape["rows"]
        expected = 1
        self.assertEqual(result, expected)

    def test_sample_three(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.sample(3).shape["rows"]
        expected = 3
        self.assertEqual(result, expected)

    def test_sample_two_percent(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.sample(0.02).shape["rows"]
        expected = 2
        self.assertEqual(result, expected)

    def test_sample_seed(self):
        df = rf.DataFrame({"foo": range(100)})
        df1 = df.sample(seed=42)
        df2 = df.sample(seed=24)
        self.assertNotEqual(df1, df2)


        """
        >>> df = DataFrame({"foo": [1, 1, 2, 2, 3], "bar": [1, -7, 5, 4, 5]})
        >>> df.sort(["bar"])
           foo  bar
        1    1   -7
        0    1    1
        3    2    4
        2    2    5
        4    3    5
        >>> df.sort(["bar"], reverse=True)
           foo  bar
        2    2    5
        4    3    5
        3    2    4
        0    1    1
        1    1   -7
        """