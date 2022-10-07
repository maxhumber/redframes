import unittest

import redframes as rf


class TestSideEffects(unittest.TestCase):
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
        self.expected = rf.DataFrame(
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
        _ = self.df.accumulate("foo", into="foo")
        self.assertEqual(self.df, self.expected)

    def test_append(self):
        df_bottom = rf.DataFrame({"foo": [10]})
        df_bottom_expected = rf.DataFrame({"foo": [10]})
        _ = self.df.append(df_bottom)
        self.assertEqual(self.df, self.expected)
        self.assertEqual(df_bottom, df_bottom_expected)

    def test_combine(self):
        _ = self.df.combine(["foo", "bar"], into="foo", sep="-")
        self.assertEqual(self.df, self.expected)

    def test_cross(self):
        _ = self.df.cross(postfix=("_a", "_b"))
        self.assertEqual(self.df, self.expected)

    def test_dedupe(self):
        _ = self.df.dedupe("baz")
        self.assertEqual(self.df, self.expected)

    def test_denix(self):
        _ = self.df.denix()
        self.assertEqual(self.df, self.expected)

    def test_drop(self):
        _ = self.df.drop("foo")
        self.assertEqual(self.df, self.expected)

    def test_fill(self):
        _ = self.df.fill("baz", direction="down")
        self.assertEqual(self.df, self.expected)

    def test_filter(self):
        _ = self.df.filter(lambda row: row["bar"] > 5)
        self.assertEqual(self.df, self.expected)

    def test_gather(self):
        _ = self.df.gather()
        self.assertEqual(self.df, self.expected)

    def test_group(self):
        _ = self.df.group("baz").rollup({"foo": ("foo", rf.stat.mean)})
        self.assertEqual(self.df, self.expected)

    def test_join(self):
        df_right = rf.DataFrame({"baz": ["A", "B"], "haz": ["Apple", "Banana"]})
        df_right_expected = rf.DataFrame(
            {"baz": ["A", "B"], "haz": ["Apple", "Banana"]}
        )
        _ = self.df.join(df_right, on="baz")
        self.assertEqual(self.df, self.expected)
        self.assertEqual(df_right, df_right_expected)

    def test_mutate(self):
        _ = self.df.mutate({"foo": lambda row: row["foo"] * 10})
        self.assertEqual(self.df, self.expected)

    def test_rank(self):
        _ = self.df.rank("bar", into="bar_rank", descending=True)
        self.assertEqual(self.df, self.expected)

    def test_rename(self):
        _ = self.df.rename({"foo": "oof"})
        self.assertEqual(self.df, self.expected)

    def test_replace(self):
        _ = self.df.replace({"baz": {"B": "Banana"}})
        self.assertEqual(self.df, self.expected)

    def test_rollup(self):
        _ = self.df.rollup({"bar_mean": ("bar", rf.stat.mean)})
        self.assertEqual(self.df, self.expected)

    def test_sample(self):
        _ = self.df.sample(5)
        self.assertEqual(self.df, self.expected)

    def test_select(self):
        _ = self.df.select(["foo", "bar"])
        self.assertEqual(self.df, self.expected)

    def test_shuffle(self):
        _ = self.df.shuffle()
        self.assertEqual(self.df, self.expected)

    def test_sort(self):
        _ = self.df.sort("bar", descending=True)
        self.assertEqual(self.df, self.expected)

    def test_split(self):
        _ = self.df.split("jaz", into=["jaz_1", "jaz_2"], sep="::")
        self.assertEqual(self.df, self.expected)

    def test_spread(self):
        _ = self.df.denix("baz").select(["baz", "foo"]).spread("baz", "foo")
        self.assertEqual(self.df, self.expected)

    def test_take(self):
        _ = self.df.take(-3)
        self.assertEqual(self.df, self.expected)
