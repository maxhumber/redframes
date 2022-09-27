import unittest

import pandas as pd

import redframes as rf


class TestInterchange(unittest.TestCase):
    def test_wrap_no_side_effect(self):
        rdf = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        result = pd.api.interchange.from_dataframe(rdf)
        expected = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.assertTrue(result.equals(expected))
