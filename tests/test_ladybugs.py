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
