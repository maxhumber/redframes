import unittest
from redframes.take import take
import pandas as pd

class TestTask(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"a": range(100)})

    def test_take(self): 
        result = take(self.df, 1)
        expected = self.df.head(1)
        equal = result.equals(expected)
        self.assertTrue(equal)

# df = pd.DataFrame({"a": range(100)})

# take(df)

# take(df, "a")

# take(df, 0)

# take(df, 3)

# take(df, -3)

# take(df, 1000)