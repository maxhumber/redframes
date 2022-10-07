import unittest

import pandas as pd

import redframes as rf


def index_is_okay(df: rf.DataFrame) -> bool:
    index = df._data.index
    is_unnamed = index.name == None
    is_range = isinstance(index, pd.RangeIndex)
    is_zero_start = index.start == 0
    is_one_step = index.step == 1
    return all([is_unnamed, is_range, is_zero_start, is_one_step])


class TestIndex(unittest.TestCase):
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

    def test_accumulate(self):
        new = self.df.accumulate("foo", into="foo")
        self.assertTrue(index_is_okay(new))

    def test_append(self):
        df_bottom = rf.DataFrame({"foo": [10]})
        new = self.df.append(df_bottom)
        self.assertTrue(index_is_okay(new))

    def test_combine(self):
        new = self.df.combine(["foo", "bar"], into="foo", sep="-")
        self.assertTrue(index_is_okay(new))

    def test_cross(self):
        new = self.df.cross()
        self.assertTrue(index_is_okay(new))

    def test_dedupe(self):
        new = self.df.dedupe("baz")
        self.assertTrue(index_is_okay(new))

    def test_denix(self):
        new = self.df.denix()
        self.assertTrue(index_is_okay(new))

    def test_drop(self):
        new = self.df.drop("foo")
        self.assertTrue(index_is_okay(new))

    def test_fill(self):
        new = self.df.fill("baz", direction="down")
        self.assertTrue(index_is_okay(new))

    def test_filter(self):
        new = self.df.filter(lambda row: row["bar"] > 5)
        self.assertTrue(index_is_okay(new))

    def test_gather(self):
        new = self.df.gather()
        self.assertTrue(index_is_okay(new))

    def test_group(self):
        new = self.df.group("baz").rollup({"foo": ("foo", rf.stat.mean)})
        self.assertTrue(index_is_okay(new))

    def test_join(self):
        df_right = rf.DataFrame({"baz": ["A", "B"], "haz": ["Apple", "Banana"]})
        new = self.df.join(df_right, on="baz")
        self.assertTrue(index_is_okay(new))

    def test_mutate(self):
        new = self.df.mutate({"foo": lambda row: row["foo"] * 10})
        self.assertTrue(index_is_okay(new))

    def test_rank(self):
        new = self.df.rank("bar", into="bar_rank", descending=True)
        self.assertTrue(index_is_okay(new))

    def test_rename(self):
        new = self.df.rename({"foo": "oof"})
        self.assertTrue(index_is_okay(new))

    def test_replace(self):
        new = self.df.replace({"baz": {"B": "Banana"}})
        self.assertTrue(index_is_okay(new))

    def test_rollup(self):
        new = self.df.rollup({"bar_mean": ("bar", rf.stat.mean)})
        self.assertTrue(index_is_okay(new))

    def test_sample(self):
        new = self.df.sample(5)
        self.assertTrue(index_is_okay(new))

    def test_select(self):
        new = self.df.select(["foo", "bar"])
        self.assertTrue(index_is_okay(new))

    def test_shuffle(self):
        new = self.df.shuffle()
        self.assertTrue(index_is_okay(new))

    def test_sort(self):
        new = self.df.sort("bar", descending=True)
        self.assertTrue(index_is_okay(new))

    def test_split(self):
        new = self.df.split("jaz", into=["jaz_1", "jaz_2"], sep="::")
        self.assertTrue(index_is_okay(new))

    def test_spread(self):
        new = self.df.denix("baz").select(["baz", "foo"]).spread("baz", "foo")
        self.assertTrue(index_is_okay(new))

    def test_take(self):
        new = self.df.take(-3)
        self.assertTrue(index_is_okay(new))
