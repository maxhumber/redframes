import unittest

import redframes as rf


class TestLadyBugs(unittest.TestCase):
    def test_gather_spread_string_values(self):
        df = rf.DataFrame(
            {"foo": ["A", "B", "C"], "bar": ["D", "E", "F"], "baz": ["G", "H", "I"]}
        )
        result = df.gather().spread("variable", "value")
        expected = rf.DataFrame(
            {"bar": ["D", "E", "F"], "baz": ["G", "H", "I"], "foo": ["A", "B", "C"]}
        )
        self.assertEqual(result, expected)

    def test_types_mixed_column(self):
        df = rf.DataFrame({"foo": [1, None, 2.0, "3"]})
        result = df.types
        expected = {"foo": object}
        self.assertEqual(result, expected)

    def test_comine_overwrite_and_drop_other(self):
        df = rf.DataFrame({"foo": [1, 2, 3], "bar": [1, 2, 3]})
        result = df.combine(["foo", "bar"], into="foo", sep="-", drop=True)
        expected = rf.DataFrame({"foo": ["1-1", "2-2", "3-3"]})
        self.assertEqual(result, expected)
