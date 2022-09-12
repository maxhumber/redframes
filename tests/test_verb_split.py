import unittest

import redframes as rf


class TestVerbCombine(unittest.TestCase):
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
