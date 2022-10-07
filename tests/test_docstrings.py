import unittest

import redframes as rf


class TestDocstrings(unittest.TestCase):
    def test_take(self):
        df = rf.DataFrame({"foo": range(10)})
        result1 = df.take(1)
        result2 = df.take(-2)
        expected1 = rf.DataFrame({"foo": [0]})
        expected2 = rf.DataFrame({"foo": [8, 9]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)

    def test_accumulate(self):
        df = rf.DataFrame({"foo": [1, 2, 3, 4]})
        result = df.accumulate("foo", into="cumsum")
        expected = rf.DataFrame({"foo": [1, 2, 3, 4], "cumsum": [1, 3, 6, 10]})
        self.assertEqual(result, expected)

    def test_rank(self):
        df = rf.DataFrame({"foo": [2, 3, 3, 99, 1000, 1, -6, 4]})
        result = df.rank("foo", into="rank", descending=True)
        expected = rf.DataFrame(
            {"foo": [2, 3, 3, 99, 1000, 1, -6, 4], "rank": [5.0, 4, 4, 2, 1, 6, 7, 3]}
        )
        self.assertEqual(result, expected)

    def test_rollup(self):
        df = rf.DataFrame({"foo": [1, 2, 3, 4, 5], "bar": [99, 100, 1, -5, 2]})
        result = df.rollup(
            {
                "fcount": ("foo", rf.stat.count),
                "fmean": ("foo", rf.stat.mean),
                "fsum": ("foo", rf.stat.sum),
                "fmax": ("foo", rf.stat.max),
                "bmedian": ("bar", rf.stat.median),
                "bmin": ("bar", rf.stat.min),
                "bstd": ("bar", rf.stat.std),
            }
        )
        expected = rf.DataFrame(
            {
                "fcount": [5.0],
                "fmean": [3.0],
                "fsum": [15.0],
                "fmax": [5.0],
                "bmedian": [2.0],
                "bmin": [-5.0],
                "bstd": [54.929955397760885],
            }
        )
        self.assertEqual(result, expected)

    def test_init(self):
        rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        self.assertTrue(True)

    def test_eq(self):
        adf = rf.DataFrame({"foo": [1]})
        bdf = rf.DataFrame({"bar": [1]})
        cdf = rf.DataFrame({"foo": [1]})
        self.assertFalse(adf == bdf)
        self.assertTrue(adf == cdf)

    def test_getitem(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        result = df["foo"]
        expected = [1, 2]
        self.assertEqual(result, expected)

    def test_str(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        result = str(df)
        expected = "rf.DataFrame({'foo': [1, 2], 'bar': ['A', 'B']})"
        self.assertEqual(result, expected)

    def test_columns(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"], "baz": [True, False]})
        result = df.columns
        expected = ["foo", "bar", "baz"]
        self.assertEqual(result, expected)

    def test_dimensions(self):
        df = rf.DataFrame({"foo": range(10), "bar": range(10, 20)})
        result = df.dimensions
        expected = {"rows": 10, "columns": 2}
        self.assertEqual(result, expected)

    def test_empty(self):
        df = rf.DataFrame()
        result = df.empty
        expected = True
        self.assertEqual(result, expected)

    def test_memory(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": ["A", "B", "C"]})
        result = df.memory
        expected = "326 B"
        self.assertEqual(result, expected)

    def test_types(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"], "baz": [True, False]})
        result = df.types
        expected = {"foo": int, "bar": object, "baz": bool}
        self.assertEqual(result, expected)

    def test_append(self):
        df1 = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        df2 = rf.DataFrame({"bar": ["C", "D"], "foo": [3, 4], "baz": ["$", "@"]})
        result = df1.append(df2)
        expected = rf.DataFrame(
            {
                "foo": [1, 2, 3, 4],
                "bar": ["A", "B", "C", "D"],
                "baz": [None, None, "$", "@"],
            }
        )
        self.assertEqual(result, expected)

    def test_combine(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        result = df.combine(["bar", "foo"], into="baz", sep="::", drop=True)
        expected = rf.DataFrame({"baz": ["A::1", "B::2"]})
        self.assertEqual(result, expected)

    def test_cross(self):
        df = rf.DataFrame({"foo": ["a", "b", "c"], "bar": [1, 2, 3]})
        dfa = rf.DataFrame({"foo": [1, 2, 3]})
        dfb = rf.DataFrame({"bar": [1, 2, 3]})
        result1 = df.cross()
        result2 = dfa.cross(dfb, postfix=("_a", "_b"))
        expected1 = rf.DataFrame(
            {
                "foo_lhs": ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
                "bar_lhs": [1, 1, 1, 2, 2, 2, 3, 3, 3],
                "foo_rhs": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
                "bar_rhs": [1, 2, 3, 1, 2, 3, 1, 2, 3],
            }
        )
        expected2 = rf.DataFrame(
            {"foo": [1, 1, 1, 2, 2, 2, 3, 3, 3], "bar": [1, 2, 3, 1, 2, 3, 1, 2, 3]}
        )
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)

    def test_dedupe(self):
        df = rf.DataFrame({"foo": [1, 1, 2, 2], "bar": ["A", "A", "B", "A"]})
        result1 = df.dedupe()
        result2 = df.dedupe("foo")
        result3 = df.dedupe(["foo", "bar"])
        expected1 = rf.DataFrame({"foo": [1, 2, 2], "bar": ["A", "B", "A"]})
        expected2 = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})
        expected3 = rf.DataFrame({"foo": [1, 2, 2], "bar": ["A", "B", "A"]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)

    def test_denix(self):
        df = rf.DataFrame(
            {"foo": [1, None, 3, None, 5, 6], "bar": [1, None, 3, 4, None, None]}
        )
        result1 = df.denix()
        result2 = df.denix("bar")
        result3 = df.denix(["foo", "bar"])
        expected1 = rf.DataFrame({"foo": [1.0, 3.0], "bar": [1.0, 3.0]})
        expected2 = rf.DataFrame({"foo": [1.0, 3.0, None], "bar": [1.0, 3.0, 4.0]})
        expected3 = rf.DataFrame({"foo": [1.0, 3.0], "bar": [1.0, 3.0]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)

    def test_drop(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
        result1 = df.drop("baz")
        result2 = df.drop(["foo", "baz"])
        expected1 = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        expected2 = rf.DataFrame({"bar": [3, 4]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)

    def test_fill(self):
        df = rf.DataFrame(
            {"foo": [1, None, None, 2, None], "bar": [None, "A", None, "B", None]}
        )
        result1 = df.fill(constant=0)
        result2 = df.fill(direction="down")
        result3 = df.fill("foo", direction="down")
        result4 = df.fill(["foo"], direction="up")
        expected1 = rf.DataFrame(
            {"foo": [1.0, 0.0, 0.0, 2.0, 0.0], "bar": [0, "A", 0, "B", 0]}
        )
        expected2 = rf.DataFrame(
            {"foo": [1.0, 1.0, 1.0, 2.0, 2.0], "bar": [None, "A", "A", "B", "B"]}
        )
        expected3 = rf.DataFrame(
            {"foo": [1.0, 1.0, 1.0, 2.0, 2.0], "bar": [None, "A", None, "B", None]}
        )
        expected4 = rf.DataFrame(
            {"foo": [1.0, 2.0, 2.0, 2.0, None], "bar": [None, "A", None, "B", None]}
        )
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)
        self.assertEqual(result4, expected4)

    def test_filter(self):
        df = rf.DataFrame({"foo": ["A", "A", "A", "B"], "bar": [1, 2, 3, 4]})
        result1 = df.filter(lambda row: row["foo"].isin(["A"]))
        result2 = df.filter(lambda row: (row["foo"] == "A") & (row["bar"] <= 2))
        result3 = df.filter(lambda row: (row["foo"] == "B") | (row["bar"] == 1))
        expected1 = rf.DataFrame({"foo": ["A", "A", "A"], "bar": [1, 2, 3]})
        expected2 = rf.DataFrame({"foo": ["A", "A"], "bar": [1, 2]})
        expected3 = rf.DataFrame({"foo": ["A", "B"], "bar": [1, 4]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)

    def test_gather(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [4, 5]})
        result1 = df.gather()
        result2 = df.gather(["foo", "bar"], into=("var", "val"))
        expected1 = rf.DataFrame(
            {
                "variable": ["foo", "foo", "bar", "bar", "baz", "baz"],
                "value": [1, 2, 3, 4, 4, 5],
            }
        )
        expected2 = rf.DataFrame(
            {
                "baz": [4, 5, 4, 5],
                "var": ["foo", "foo", "bar", "bar"],
                "val": [1, 2, 3, 4],
            }
        )
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)

    def test_group(self):
        df = rf.DataFrame(
            {
                "foo": ["A", "A", "A", "B", "B"],
                "bar": [1, 2, 3, 4, 5],
                "baz": [9, 7, 7, 5, 6],
            }
        )
        result1 = df.group("foo").accumulate("bar", into="bar_cumsum")
        result2 = df.group("foo").rank("baz", into="baz_rank", descending=True)
        result3 = df.group("foo").rollup(
            {"bar_mean": ("bar", rf.stat.mean), "baz_min": ("baz", rf.stat.min)}
        )
        result4 = df.group("foo").take(1)
        expected1 = rf.DataFrame(
            {
                "foo": ["A", "A", "A", "B", "B"],
                "bar": [1, 2, 3, 4, 5],
                "baz": [9, 7, 7, 5, 6],
                "bar_cumsum": [1, 3, 6, 4, 9],
            }
        )
        expected2 = rf.DataFrame(
            {
                "foo": ["A", "A", "A", "B", "B"],
                "bar": [1, 2, 3, 4, 5],
                "baz": [9, 7, 7, 5, 6],
                "baz_rank": [1.0, 2.0, 2.0, 2.0, 1.0],
            }
        )
        expected3 = rf.DataFrame(
            {"foo": ["A", "B"], "bar_mean": [2.0, 4.5], "baz_min": [7, 5]}
        )
        expected4 = rf.DataFrame({"foo": ["A", "B"], "bar": [1, 4], "baz": [9, 5]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)
        self.assertEqual(result4, expected4)

    def test_join(self):
        adf = rf.DataFrame({"foo": ["A", "B", "C"], "bar": [1, 2, 3]})
        bdf = rf.DataFrame({"foo": ["A", "B", "D"], "baz": ["!", "@", "#"]})
        result1 = adf.join(bdf, on="foo", how="left")
        result2 = adf.join(bdf, on="foo", how="right")
        result3 = adf.join(bdf, on="foo", how="inner")
        result4 = adf.join(bdf, on="foo", how="full")
        expected1 = rf.DataFrame(
            {"foo": ["A", "B", "C"], "bar": [1, 2, 3], "baz": ["!", "@", None]}
        )
        expected2 = rf.DataFrame(
            {"foo": ["A", "B", "D"], "bar": [1.0, 2.0, None], "baz": ["!", "@", "#"]}
        )
        expected3 = rf.DataFrame({"foo": ["A", "B"], "bar": [1, 2], "baz": ["!", "@"]})
        expected4 = rf.DataFrame(
            {
                "foo": ["A", "B", "C", "D"],
                "bar": [1.0, 2.0, 3.0, None],
                "baz": ["!", "@", None, "#"],
            }
        )
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)
        self.assertEqual(result4, expected4)

    def test_mutate(self):
        df = rf.DataFrame({"foo": [1, 2, 3]})
        result = df.mutate(
            {
                "bar": lambda row: float(row["foo"]),
                "baz": lambda row: "X" + str(row["bar"] * 2),
                "jaz": lambda _: "Jazz",
            }
        )
        expected = rf.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [1.0, 2.0, 3.0],
                "baz": ["X2.0", "X4.0", "X6.0"],
                "jaz": ["Jazz", "Jazz", "Jazz"],
            }
        )
        self.assertEqual(result, expected)

    def test_rename(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        result = df.rename({"foo": "oof", "bar": "rab"})
        expected = rf.DataFrame({"oof": [1, 2], "rab": [3, 4]})
        self.assertEqual(result, expected)

    def test_replace(self):
        df = rf.DataFrame({"foo": [1, 2, 2, 2, 1], "bar": [1, "A", "B", True, False]})
        result = df.replace(
            {"foo": {2: 222}, "bar": {False: 0, True: 1, "A": 2, "B": 3}}
        )
        expected = rf.DataFrame({"foo": [1, 222, 222, 222, 1], "bar": [1, 2, 3, 1, 0]})
        self.assertEqual(result, expected)

    def test_sample(self):
        df = rf.DataFrame({"foo": range(10), "bar": range(10, 20)})
        result1 = df.sample(1)
        result2 = df.sample(3)
        result3 = df.sample(0.3)
        self.assertEqual(len(result1), 1)
        self.assertEqual(len(result2), 3)
        self.assertEqual(len(result3), 3)

    def test_select(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
        result1 = df.select("foo")
        result2 = df.select(["foo", "baz"])
        expected1 = rf.DataFrame({"foo": [1, 2]})
        expected2 = rf.DataFrame({"foo": [1, 2], "baz": [5, 6]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)

    def test_shuffle(self):
        df = rf.DataFrame({"foo": range(5), "bar": range(5, 10)})
        result = df.shuffle()
        self.assertNotEqual(df, result)

    def test_sort(self):
        df = rf.DataFrame({"foo": ["Z", "X", "A", "A"], "bar": [2, -2, 4, -4]})
        result1 = df.sort("bar")
        result2 = df.sort("bar", descending=True)
        result3 = df.sort(["foo", "bar"], descending=False)
        expected1 = rf.DataFrame({"foo": ["A", "X", "Z", "A"], "bar": [-4, -2, 2, 4]})
        expected2 = rf.DataFrame({"foo": ["A", "Z", "X", "A"], "bar": [4, 2, -2, -4]})
        expected3 = rf.DataFrame({"foo": ["A", "A", "X", "Z"], "bar": [-4, 4, -2, 2]})
        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)
        self.assertEqual(result3, expected3)

    def test_split(self):
        df = rf.DataFrame({"foo": ["A::1", "B::2", "C:3"]})
        result = df.split("foo", into=["foo", "bar"], sep="::", drop=True)
        expected = rf.DataFrame({"foo": ["A", "B", "C:3"], "bar": ["1", "2", None]})
        self.assertEqual(result, expected)

    def test_spread(self):
        df = rf.DataFrame(
            {"foo": ["A", "A", "A", "B", "B", "B", "B"], "bar": [1, 2, 3, 4, 5, 6, 7]}
        )
        result = df.spread("foo", using="bar")
        expected = rf.DataFrame({"A": [1.0, 2.0, 3.0, None], "B": [4.0, 5.0, 6.0, 7.0]})
        self.assertEqual(result, expected)
