import unittest

import redframes as rf


class TestDeprecations(unittest.TestCase):
    def test_summarize_deprecation(self):
        df = rf.DataFrame({"foo": range(10)})
        expected = rf.DataFrame({"foo": [4.5]})
        message = "Marked for removal, please use `rollup` instead"
        with self.assertWarnsRegex(FutureWarning, message):
            result = df.summarize({"foo": ("foo", rf.stat.mean)})
            self.assertEqual(result, expected)

    def test_gather_beside_deprecation(self):
        df = rf.DataFrame({"foo": [1, 1, 2, 2], "bar": [1, 2, 3, 4]})
        expected = rf.DataFrame(
            {
                "foo": [1, 1, 2, 2],
                "variable": ["bar", "bar", "bar", "bar"],
                "value": [1, 2, 3, 4],
            }
        )
        with self.assertWarnsRegex(FutureWarning, "Marked for removal*"):
            result = df.gather(beside="foo")
            self.assertEqual(result, expected)
