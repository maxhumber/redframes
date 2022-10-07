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
