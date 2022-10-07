import unittest

import redframes as rf


class TestDupeColumns(unittest.TestCase):
    def setUp(self):
        self.df = rf.DataFrame(
            {
                "foo": range(10),
                "bar": [1, 3.2, 4.5, 2, -1, 30, None, 1.1, 1.1, 9],
                "baz": ["A", "A", None, "B", "B", "A", "B", "C", "C", "A"],
                "jaz": [
                    "1::1",
                    "2::2",
                    "3:3",
                    "4::4",
                    "5::5",
                    "6::7",
                    "7::8",
                    "8::9",
                    "9::0",
                    "0::-1",
                ],
                "raz": [1, 2, 3, None, None, None, 9, 9, None, None],
            }
        )

    def test_accumulate_not_unqiue(self):
        self.df.accumulate("foo", into="foo")
        self.assertTrue(True)

    def test_accumulate_overwrite_existing(self):
        with self.assertWarnsRegex(UserWarning, "overwriting existing column *"):
            self.df.accumulate("foo", into="bar")

    def test_combine_into_overwrite(self):
        self.df.combine(["foo", "bar"], into="foo", sep="-")
        self.assertTrue(True)

    def test_combine_overwrite_existing(self):
        with self.assertWarnsRegex(UserWarning, "overwriting existing column *"):
            self.df.combine(["foo", "bar"], into="baz", sep="-")

    def test_combine_overwrite_no_drop(self):
        self.df.combine(["foo", "bar"], into="foo", sep="-", drop=False)
        self.assertTrue(True)

    def test_gather_same_column_names(self):
        with self.assertRaisesRegex(TypeError, "must be unique"):
            self.df.gather(into=("foo", "foo"))

    def test_gather_exising_column_name_for_variable(self):
        with self.assertRaisesRegex(TypeError, "must not be an existing column key"):
            self.df.gather(into=("foo", "value"))

    def test_gather_exising_column_name_for_value(self):
        with self.assertRaisesRegex(TypeError, "must not be an existing column key"):
            self.df.gather(into=("variable", "foo"))

    def test_gather_exising_column_key(self):
        with self.assertRaisesRegex(TypeError, "must not be an existing column key"):
            self.df.gather(["foo", "bar"], into=("raz", "baz"))

    def test_rank_into_overwrite(self):
        self.df.rank("bar", into="bar", descending=True)
        self.assertTrue(True)

    def test_rank_overwrite_existing(self):
        with self.assertWarnsRegex(UserWarning, "overwriting existing column *"):
            self.df.rank("bar", into="baz", descending=True)

    def test_rename_duplicated_dict_values(self):
        with self.assertRaisesRegex(KeyError, "columns must be unique"):
            self.df.rename({"foo": "oof", "bar": "oof"})

    def test_rollup_group_existing_column(self):
        with self.assertRaisesRegex(ValueError, "cannot insert *"):
            self.df.group("baz").rollup({"baz": ("foo", rf.stat.max)})

    def test_select_duplicate_keys(self):
        with self.assertRaisesRegex(KeyError, "column keys must be unique"):
            self.df.select(["foo", "foo"])

    def test_split_overwrite_into_one(self):
        self.df.split("jaz", into=["jaz", "paz"], sep="::")
        self.assertTrue(True)

    def test_split_overwrite_into_existing(self):
        with self.assertRaisesRegex(KeyError, "into keys must be unique"):
            self.df.split("jaz", into=["jaz", "foo"], sep="::")

    def test_split_duplicated_into_keys(self):
        with self.assertRaisesRegex(KeyError, "into keys must be unique"):
            self.df.split("jaz", into=["paz", "paz"], sep="::")

    def test_spread_duplicated_column_names(self):
        with self.assertRaisesRegex(KeyError, "column and using must be unique"):
            self.df.gather().spread("variable", "variable")
