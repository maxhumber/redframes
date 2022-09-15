import unittest

import redframes as rf


class TestVerbCombine(unittest.TestCase):
    def test_columns_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "must be list"):
            df.combine("foo", into="foo", sep="-")

    def test_sep_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "must be str"):
            df.combine(["foo", "bar"], into="baz", sep=1)

    def test_into_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "must be str"):
            df.combine(["foo", "bar"], into=["baz"], sep="_")

    def test_drop_type_bad(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        with self.assertRaisesRegex(TypeError, "must be bool"):
            df.combine(["foo", "bar"], into="baz", sep="_", drop=2)

    def test_two_columns(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        result = df.combine(["foo", "bar"], into="baz", sep="_")
        expected = rf.DataFrame({"baz": ["1_4", "2_5", "3_6"]})
        self.assertEqual(result, expected)

    def test_three_columns(self):
        df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4], "baz": [5, 6]})
        result = df.combine(["foo", "bar", "baz"], into="all", sep=":")
        expected = rf.DataFrame({"all": ["1:3:5", "2:4:6"]})
        self.assertEqual(result, expected)
