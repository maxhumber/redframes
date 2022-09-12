import unittest

import redframes as rf


class TestVerbAppend(unittest.TestCase):
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

    def test_dedupe_one_column(self):
        df = rf.DataFrame({"foo": [1, 1, 1, 2, 2, 2], "bar": [1, 2, 3, 4, 5, 5]})
        result = df.dedupe(["foo"])
        expected = rf.DataFrame({"foo": [1, 2], "bar": [1, 4]})
        self.assertEqual(result, expected)
