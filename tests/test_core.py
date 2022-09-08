import unittest
from pathlib import Path
from shutil import rmtree as delete
from tempfile import mkdtemp as make_temp_dir

import pandas as pd

import redframes as rf


class TestCore(unittest.TestCase):

    def test_init_good_dict(self):
        df = rf.DataFrame({"foo": [1, 2, 3]})
        self.assertIsNotNone(df)

    def test_init_bad_dict_values_not_in_list(self):
        message = "If using all scalar values, you must pass an index"
        with self.assertRaisesRegex(ValueError, message):
            rf.DataFrame({"foo": 0})

    def test_init_bad_dict_mismatched_lengths(self):
        message = "All arrays must be of the same length"
        with self.assertRaisesRegex(ValueError, message):
            rf.DataFrame({"foo": [0], "bar": [1, 2]})

    # def test_init_pandas_dataframe(self):
    #     pdf = pd.DataFrame({"foo": [1, 2, 3]})
    #     rdf = rf.DataFrame(pdf)
    #     self.assertIsNotNone(rdf)

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
        data = {"foo": ["a", "b", "c"], "bar": [1, 2, 3], "baz": [4.0, None, 5.6]}
        df = rf.DataFrame(data)
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
        message = "Rows argument exceeds total number of rows"
        with self.assertRaisesRegex(ValueError, message):
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

    def test_sample_plus_float(self):
        df = rf.DataFrame({"foo": range(100)})
        with self.assertRaisesRegex(ValueError, "Rows argument must be an int if >= 1"):
            df.sample(1.2)

    def test_shuffle(self):
        df = rf.DataFrame({"foo": range(100)})
        result = df.shuffle()
        self.assertNotEqual(df, result)

    def test_shuffle_seed(self):
        df = rf.DataFrame({"foo": range(100)})
        df1b = df.shuffle(seed=42)
        df1a = df.shuffle(seed=42)
        df2 = df.shuffle(seed=24)
        self.assertEqual(df1a, df1b)
        self.assertNotEqual(df1a, df2)

    def test_sort_bad_columns(self):
        df = rf.DataFrame({"foo": [1, 1, 2, 2, 3], "bar": [1, -7, 5, 4, 5]})
        with self.assertRaisesRegex(TypeError, "Invalid columns argument *"):
            df.sort("foo")

    def test_sort_one_column(self):
        df = rf.DataFrame({"foo": [1, 1, 2, 2, 3, -1], "bar": [1, -7, 5, 4, 5, 6]})
        result = df.sort(["bar"])
        expected = rf.DataFrame(
            {"foo": [1, 1, 2, 2, 3, -1], "bar": [-7, 1, 4, 5, 5, 6]}
        )
        self.assertEqual(result, expected)

    def test_sort_two_columns(self):
        df = rf.DataFrame({"foo": [1, 1, 2, 2, 3, -1], "bar": [1, -7, 5, 4, 5, 6]})
        result = df.sort(["bar", "foo"])
        expected = rf.DataFrame(
            {"foo": [1, 1, 2, 2, 3, -1], "bar": [-7, 1, 4, 5, 5, 6]}
        )
        self.assertEqual(result, expected)

    def test_sort_two_columns_order(self):
        df = rf.DataFrame({"foo": [1, 1, 2, 2, 3, -1], "bar": [1, -7, 5, 4, 5, 6]})
        result = df.sort(["foo", "bar"])
        expected = rf.DataFrame(
            {"foo": [-1, 1, 1, 2, 2, 3], "bar": [6, -7, 1, 4, 5, 5]}
        )
        self.assertEqual(result, expected)

    def test_sort_reverse(self):
        df = rf.DataFrame({"foo": range(5)})
        result = df.sort(["foo"], reverse=True)
        expected = rf.DataFrame({"foo": [4, 3, 2, 1, 0]})
        self.assertEqual(result, expected)

    def test_filter_bad_type(self):
        df = rf.DataFrame({"foo": range(10)})
        message = "Must be a 'rowwise' function that returns a bool"
        with self.assertRaisesRegex(TypeError, message):
            df.filter(">= 3")

    def test_filter_bad_column_argument(self):
        df = rf.DataFrame({"foo": range(10)})
        with self.assertRaises(KeyError):
            df.filter(lambda row: row["bar"] <= 3)

    def test_filter_one_column(self):
        df = rf.DataFrame({"foo": range(10)})
        result = df.filter(lambda row: row["foo"] <= 3)
        expected = rf.DataFrame({"foo": [0, 1, 2, 3]})
        self.assertEqual(result, expected)

    def test_filter_multiple_columns(self):
        df = rf.DataFrame({"foo": [1, 2, 2, 2, 2, 3], "bar": [1, 2, 3, 4, 5, 6]})
        result = df.filter(lambda d: (d["foo"] == 2) & (d["bar"] <= 4))
        expected = rf.DataFrame({"foo": [2, 2, 2], "bar": [2, 3, 4]})
        self.assertEqual(result, expected)

    def test_dedupe_default(self):
        df = rf.DataFrame({"foo": [1, 1, 1, 2, 2, 2], "bar": [1, 2, 3, 4, 5, 5]})
        result = df.dedupe()
        expected = rf.DataFrame({"foo": [1, 1, 1, 2, 2], "bar": [1, 2, 3, 4, 5]})
        self.assertEqual(result, expected)

    def test_dedupe_bad_type(self):
        df = rf.DataFrame({"foo": range(10)})
        with self.assertRaisesRegex(TypeError, "Invalid columns argument *"):
            df.dedupe("foo")

    def test_dedupe_bad_column(self):
        df = rf.DataFrame({"foo": range(10)})
        with self.assertRaises(KeyError):
            df.dedupe(["bar"])

    def test_dedupe_bad_keep_argument(self):
        df = rf.DataFrame({"foo": range(10)})
        message = "keep argument must be one of {'first', 'last'}"
        with self.assertRaisesRegex(ValueError, message):
            df.dedupe(["foo"], keep="anything")

    def test_dedupe_one_column(self):
        df = rf.DataFrame({"foo": [1, 1, 1, 2, 2, 2], "bar": [1, 2, 3, 4, 5, 5]})
        result = df.dedupe(["foo"])
        expected = rf.DataFrame({"foo": [1, 2], "bar": [1, 4]})
        self.assertEqual(result, expected)

    def test_dedupe_keep_last(self):
        df = rf.DataFrame({"foo": [1, 1, 1, 2, 2, 2], "bar": [1, 2, 3, 4, 5, 5]})
        result = df.dedupe(["foo"], keep="last")
        expected = rf.DataFrame({"foo": [1, 2], "bar": [3, 5]})
        self.assertEqual(result, expected)

    def test_denix_default(self):
        data = {"foo": [0, 0, None, None, 0], "bar": [1, None, None, None, 2]}
        df = rf.DataFrame(data)
        result = df.denix()
        # Annoying that pandas can't mix Nones + Ints
        expected = rf.DataFrame({"foo": [0.0, 0], "bar": [1.0, 2]})
        self.assertEqual(result, expected)

    def test_denix_bad_type(self):
        data = {"foo": [0, 0, None, None, 0], "bar": [1, None, None, None, 2]}
        df = rf.DataFrame(data)
        with self.assertRaisesRegex(TypeError, "Invalid columns argument *"):
            df.denix("foo")

    def test_denix_bad_column(self):
        df = rf.DataFrame({"foo": [0, 0, None, None, 0]})
        with self.assertRaises(KeyError):
            df.denix(["bar"])

    def test_denix_one_column(self):
        data = {"foo": [0, 0, None, None, 0], "bar": [1, None, None, None, 2]}
        df = rf.DataFrame(data)
        result = df.denix(["foo"])
        expected = rf.DataFrame({"foo": [0.0, 0, 0], "bar": [1, None, 2]})
        self.assertEqual(result, expected)

    def test_fill_default_down(self):
        df = rf.DataFrame({"foo": [1, None, None, 2], "bar": [None, 2, 1, None]})
        result = df.fill()
        expected = rf.DataFrame({"foo": [1.0, 1, 1, 2], "bar": [None, 2, 1, 1]})
        self.assertEqual(result, expected)

    def test_fill_up(self):
        df = rf.DataFrame({"foo": [1, None, None, 2], "bar": [None, 2, 1, None]})
        result = df.fill(strategy="up")
        expected = rf.DataFrame({"foo": [1.0, 2, 2, 2], "bar": [2, 2, 1, None]})
        self.assertEqual(result, expected)

    def test_fill_bad_columns_type(self):
        df = rf.DataFrame({"foo": [1, None, None, 2, 3, None]})
        with self.assertRaisesRegex(TypeError, "Invalid columns argument *"):
            df.fill("foo")

    def test_fill_bad_strategy(self):
        df = rf.DataFrame({"foo": [1, None, None, 2, 3, None]})
        message = "Invalid strategy, must be one of {'down', 'up', 'constant}"
        with self.assertRaisesRegex(ValueError, message):
            df.fill(["foo"], strategy="anything")

    def test_fill_single_column(self):
        df = rf.DataFrame({"foo": [1, None, None, 2], "bar": [None, 2, 1, None]})
        result = df.fill(["foo"], strategy="up")
        expected = rf.DataFrame({"foo": [1.0, 2, 2, 2], "bar": [None, 2, 1, None]})
        self.assertEqual(result, expected)

    def test_fill_constant_missing_constant(self):
        df = rf.DataFrame({"foo": [1, None, None, 2], "bar": [None, 2, 1, None]})
        message = "strategy='constant' requires a corresponding constant= argument"
        with self.assertRaisesRegex(ValueError, message):
            df.fill(["foo"], strategy="constant")

    def test_fill_constant(self):
        df = rf.DataFrame({"foo": [1, None, None, 2], "bar": [None, 2, 1, None]})
        result = df.fill(["foo"], strategy="constant", constant=3)
        expected = rf.DataFrame({"foo": [1.0, 3, 3, 2], "bar": [None, 2, 1, None]})
        self.assertEqual(result, expected)

    def test_replace_bad_argument_type(self):
        df = rf.DataFrame({"foo": ["a", "b", "c", "a"], "bar": [1, 2, None, 3]})
        with self.assertRaisesRegex(TypeError, "Invalid rules type *"):
            df.replace(["foo"])

    def test_replace_bad_column(self):
        df = rf.DataFrame({"foo": ["a", "b", "c", "a"], "bar": [1, 2, None, 3]})
        with self.assertRaisesRegex(KeyError, "Invalid columns *"):
            df.replace({"baz": {"a": "b"}})

    def test_replace_single(self):
        df = rf.DataFrame({"foo": ["a", "b", "c", "a"], "bar": [1, 2, None, 3]})
        result = df.replace({"foo": {"a": "z"}})
        expected = rf.DataFrame({"foo": ["z", "b", "c", "z"], "bar": [1, 2, None, 3]})
        self.assertEqual(result, expected)

    def test_replace_mixed_types(self):
        df = rf.DataFrame({"foo": ["a", "b", "c", "a"], "bar": [1, 2, None, 3]})
        result = df.replace({"bar": {1: "z"}})
        expected = rf.DataFrame({"foo": ["a", "b", "c", "a"], "bar": ["z", 2, None, 3]})
        self.assertEqual(result, expected)

    def test_replace_multiple(self):
        df = rf.DataFrame({"foo": ["a", "b", "c", "a"], "bar": [1, 2, None, 3]})
        result = df.replace({"foo": {"a": "z", "b": "y"}, "bar": {1: 0, 3: 0}})
        expected = rf.DataFrame({"foo": ["z", "y", "c", "z"], "bar": [0, 2, None, 0]})
        self.assertEqual(result, expected)

    def test_rename_bad_type(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with self.assertRaisesRegex(TypeError, "Invalid columns type *"):
            df.rename("foo")

    def test_rename_bad_columns(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with self.assertRaisesRegex(KeyError, "Invalid columns *"):
            df.rename({"jaz": "haz"})

    def test_rename_single(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        result = df.rename({"baz": "jaz"})
        expected = rf.DataFrame({"foo": [1], "bar": [2], "jaz": [3]})
        self.assertEqual(result, expected)

    def test_rename_multiple(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        result = df.rename({"foo": "oof", "bar": "rab", "baz": "zab"})
        expected = rf.DataFrame({"oof": [1], "rab": [2], "zab": [3]})
        self.assertEqual(result, expected)

    def test_select_bad_columns(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with self.assertRaises(KeyError):
            df.select(["a", "y", "z"])

    def test_select_bad_type_argument(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with self.assertRaisesRegex(TypeError, "Invalid columns type *"):
            df.select("foo")

    def test_select_multiple_repeated(self):
        pass

    def test_select_multiple(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        result = df.select(["foo", "bar"])
        expected = rf.DataFrame({"foo": [1], "bar": [2]})
        self.assertEqual(result, expected)

    def test_select_single(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        result = df.select(["baz"])
        expected = rf.DataFrame({"baz": [3]})
        self.assertEqual(result, expected)

    def test_remove_bad_columns(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with self.assertRaises(KeyError):
            df.remove(["a", "y", "z"])

    def test_remove_bad_type_argument(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with self.assertRaisesRegex(TypeError, "Invalid columns type *"):
            df.remove("foo")

    def test_remove_multiple(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        result = df.remove(["foo", "bar"])
        expected = rf.DataFrame({"baz": [3]})
        self.assertEqual(result, expected)

    def test_remove_single(self):
        df = rf.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        result = df.remove(["baz"])
        expected = rf.DataFrame({"foo": [1], "bar": [2]})
        self.assertEqual(result, expected)

    def test_mutate_bad_type(self):
        df = rf.DataFrame({"foo": range(10)})
        with self.assertRaisesRegex(TypeError, "Must be a dictionary of mutations"):
            df.mutate("foo")

    def test_mutate_single(self):
        df = rf.DataFrame({"foo": range(3)})
        result = df.mutate({"bar": lambda row: row["foo"] * 2})
        expected = rf.DataFrame({"foo": [0, 1, 2], "bar": [0, 2, 4]})
        self.assertEqual(result, expected)

    def test_mutate_multiple_and_after(self):
        df = rf.DataFrame({"foo": [0.0, 1, 2]})
        result = df.mutate(
            {"bar": lambda row: row["foo"] * 4, "baz": lambda row: row["bar"] / 2}
        )
        expected = rf.DataFrame(
            {"foo": [0.0, 1, 2], "bar": [0.0, 4, 8], "baz": [0.0, 2, 4]}
        )
        self.assertEqual(result, expected)

    def test_mutate_with_none(self):
        df = rf.DataFrame({"foo": [1, None, 2, 3]})
        result = df.mutate({"bar": lambda row: row["foo"] * 5})
        expected = rf.DataFrame({"foo": [1, None, 2, 3], "bar": [5, None, 10, 15]})
        self.assertEqual(result, expected)

    def test_mutate_strings(self):
        df = rf.DataFrame({"foo": ["a", "a", None, "b", "a"]})

        def replace(row):
            value = row["foo"]
            replacements = {"a": "x", "b": "y"}
            replacement = replacements.get(value)
            return replacement

        result = df.mutate({"bar": replace})
        expected = rf.DataFrame(
            {"foo": ["a", "a", None, "b", "a"], "bar": ["x", "x", None, "y", "x"]}
        )
        self.assertEqual(result, expected)

    def test_mutate_multiple_column_references(self):
        df = rf.DataFrame({"foo": range(10, 10 + 5), "bar": range(3, 3 + 5)})
        result = df.mutate({"baz": lambda row: row["foo"] - row["bar"]})
        expected = rf.DataFrame(
            {"foo": range(10, 10 + 5), "bar": range(3, 3 + 5), "baz": [7] * 5}
        )
        self.assertEqual(result, expected)

    def test_mutate_overwrite_column(self):
        df = rf.DataFrame({"foo": range(5)})
        result = df.mutate({"foo": lambda row: row["foo"] * 2})
        expected = rf.DataFrame({"foo": map(lambda x: x * 2, range(5))})
        self.assertEqual(result, expected)

    def test_mutate_retype(self):
        df = rf.DataFrame({"foo": range(5)})
        original_type = df.types["foo"]
        df = df.mutate({"foo": str})
        new_type = df.types["foo"]
        self.assertEqual(original_type, int)
        self.assertEqual(new_type, str)

    def test_split_bad_column_type(self):
        df = rf.DataFrame({"foo": ["foo-1", "foo-2", "foo-3"]})
        with self.assertRaisesRegex(TypeError, "column argument must be a string"):
            df.split(["foo"], sep="-", into=["foo"])

    def test_split_bad_sep_type(self):
        df = rf.DataFrame({"foo": ["foo-1", "foo-2", "foo-3"]})
        with self.assertRaisesRegex(TypeError, "sep= separator must be a string"):
            df.split("foo", sep=1, into=["foo"])

    def test_split_bad_into_type(self):
        df = rf.DataFrame({"foo": ["foo-1", "foo-2", "foo-3"]})
        with self.assertRaisesRegex(TypeError, "into= columns argument must be a list"):
            df.split("foo", sep="-", into="foo")

    def test_split_bad_into_wrong_length(self):
        df = rf.DataFrame({"foo": ["foo-1", "foo-2", "foo-3"]})
        with self.assertRaisesRegex(ValueError, "Columns must be same length as key"):
            df.split("foo", sep="-", into=["foo"])

    def test_split_easy(self):
        df = rf.DataFrame({"foo": ["foo-1", "foo-2", "foo-3"]})
        result = df.split("foo", sep="-", into=["foo", "bar"])
        expected = rf.DataFrame({"foo": ["foo"] * 3, "bar": ["1", "2", "3"]})
        self.assertEqual(result, expected)

    def test_split_more_than_one(self):
        df = rf.DataFrame({"foo": ["0:1:2", "3:4:5"]})
        result = df.split("foo", sep=":", into=["foo", "bar", "baz"])
        expected = rf.DataFrame(
            {"foo": ["0", "3"], "bar": ["1", "4"], "baz": ["2", "5"]}
        )
        self.assertEqual(result, expected)

    def test_split_multiple_with_overflow(self):
        df = rf.DataFrame({"foo": ["0:1:2", "3:4:5:6"]})
        result = df.split("foo", sep=":", into=["foo", "bar", "baz", "jaz"])
        expected = rf.DataFrame(
            {
                "foo": ["0", "3"],
                "bar": ["1", "4"],
                "baz": ["2", "5"],
                "jaz": [None, "6"],
            }
        )
        self.assertEqual(result, expected)

    def test_join_bad_rhs_type(self):
        df = rf.DataFrame({"foo": [1, 2, 3]})
        with self.assertRaisesRegex(TypeError, "rhs must be a rf.DataFrame"):
            df.join(1, columns={"foo", "foo"})
