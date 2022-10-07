import unittest

import redframes as rf


class TestTypeHints(unittest.TestCase):
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

    def test_io_load_bad_path(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            rf.load(1)

    def test_io_load_bad_file_type(self):
        with self.assertRaisesRegex(TypeError, "must end in .csv"):
            rf.load("example.json")

    def test_io_save_bad_object(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.save(1, "example.csv")

    def test_io_save_bad_path(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            rf.save(self.df, 1)

    def test_io_save_bad_format(self):
        with self.assertRaisesRegex(TypeError, "must end in .csv"):
            rf.save(self.df, "example.json")

    def test_io_unwrap_bad_object(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.unwrap(1)

    def test_io_wrap_bad_object(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.unwrap(1)

    def test_take_bad_rows(self):
        with self.assertRaisesRegex(TypeError, "must be int"):
            self.df.take("A")

    def test_accumulate_bad_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.accumulate(1, "foo")

    def test_accumulate_bad_into_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.accumulate("foo", 1)

    def test_rank_bad_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.rank(1, "bar2")

    def test_rank_bad_into_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.rank("bar", 1)

    def test_rank_bad_descending_argument(self):
        with self.assertRaisesRegex(TypeError, "must be bool"):
            self.df.rank("bar", "bar", descending="bar")

    def test_rollup_bad_over(self):
        with self.assertRaisesRegex(TypeError, "must be dict"):
            self.df.rollup(1)

    def test_rollup_bad_over_values(self):
        with self.assertRaises(TypeError):
            self.df.rollup({"bar_mean": 1})

    def test_init_bad_data(self):
        with self.assertRaisesRegex(TypeError, "must be dict | None"):
            rf.DataFrame(1)

    def test_eq_bad_rhs_object(self):
        self.assertFalse(self.df == 1)

    def test_getitem_bad_key(self):
        pass

    def test_append_bad_other(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            self.df.append(1)

    def test_combine_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list"):
            self.df.combine(1, "foo", sep="-")

    def test_combine_bad_into_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.combine(["foo", "bar"], 1, sep="-")

    def test_combine_bad_sep_argument(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.combine(["foo", "bar"], "foo", sep=1)

    def test_combine_bad_drop_argument(self):
        with self.assertRaisesRegex(TypeError, "must be bool"):
            self.df.combine(["foo", "bar"], "foo", sep=":::", drop="A")

    def test_dedupe_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str | None"):
            self.df.dedupe(1)

    def test_denix_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str | None"):
            self.df.denix(1)

    def test_drop_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str | None"):
            self.df.drop(1)

    def test_fill_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str | None"):
            self.df.fill(1)

    def test_fill_bad_direction(self):
        with self.assertRaisesRegex(ValueError, "must be one of {'down', 'up'}"):
            self.df.fill("bar", direction="sideways")

    def test_fill_bad_constant_and_direction(self):
        with self.assertRaisesRegex(
            ValueError, "either direction OR constant must not be None"
        ):
            self.df.fill("bar")

    def test_fill_bad_no_constant_nor_direction(self):
        with self.assertRaisesRegex(
            ValueError, "either direction OR constant must be None"
        ):
            self.df.fill("bar", direction="down", constant="X")

    def test_filter_bad_func(self):
        with self.assertRaisesRegex(TypeError, "must be Func"):
            self.df.filter(1)

    def test_gather_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | None"):
            self.df.gather(1)

    def test_gather_bad_into_column(self):
        with self.assertRaisesRegex(TypeError, "must be tuple"):
            self.df.gather(["foo", "bar"], into=1)

    def test_group_bad_by_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str"):
            self.df.group(1)

    def test_join_bad_rhs_object(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            self.df.join(1, on="baz")

    def test_join_bad_on_type(self):
        rhs = rf.DataFrame()
        with self.assertRaisesRegex(TypeError, "must be list | str"):
            self.df.join(rhs, on=1)

    def test_join_bad_how_argument(self):
        rhs = rf.DataFrame()
        message = (
            "on argument is invalid, must be one of {'left', 'right', 'inner', 'full'}"
        )
        with self.assertRaisesRegex(ValueError, message):
            self.df.join(rhs, on="baz", how="inside")

    def test_mutate_bad_over(self):
        with self.assertRaisesRegex(TypeError, "must be dict"):
            self.df.mutate(1)

    def test_rename_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be dict"):
            self.df.rename(1)

    def test_rename_bad_columns_values(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.rename({"foo": 1})

    def test_replace_bad_over(self):
        with self.assertRaisesRegex(TypeError, "must be dict"):
            self.df.replace(1)

    def test_sample_bad_rows(self):
        with self.assertRaisesRegex(TypeError, "must be int | float"):
            self.df.sample("A")

    def test_select_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str"):
            self.df.select(1)

    def test_shuffle(self):
        pass

    def test_sort_bad_columns(self):
        with self.assertRaisesRegex(TypeError, "must be list | str"):
            self.df.sort(1)

    def test_sort_bad_descending_argument(self):
        with self.assertRaisesRegex(TypeError, "must be bool"):
            self.df.sort("bar", descending="A")

    def test_split_bad_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.split(1, into=["jaz1", "jaz2"], sep="::")

    def test_split_bad_into_column(self):
        with self.assertRaisesRegex(TypeError, "must be list"):
            self.df.split("jaz", into=1, sep="::")

    def test_split_bad_sep_argument(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.split("jaz", into=["jaz1", "jaz2"], sep=1)

    def test_split_bad_drop_argument(self):
        with self.assertRaisesRegex(TypeError, "must be bool"):
            self.df.split("jaz", into=["jaz1", "jaz2"], sep="::", drop="A")

    def test_spread_bad_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.spread(1, using="bar")

    def test_spread_bad_using_column(self):
        with self.assertRaisesRegex(TypeError, "must be str"):
            self.df.spread("foo", using=1)
