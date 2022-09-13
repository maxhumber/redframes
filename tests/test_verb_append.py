import unittest

import redframes as rf


class TestVerbAppend(unittest.TestCase):
    def test_no_side_effects(self):
        df1 = rf.DataFrame({"foo": [1, 2]})
        df2 = rf.DataFrame({"foo": [3, 4]})
        df1_start = df1
        df2_start = df2
        _ = df1.append(df2)
        self.assertEqual(df1_start, df1)
        self.assertEqual(df2_start, df2)

    def test_no_bad_index(self):
        df1 = rf.DataFrame({"foo": range(100)})
        df2 = rf.DataFrame({"foo": range(100, 200)})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": range(200)})
        self.assertEqual(result, expected)

    def test_bad_type(self):
        df = rf.DataFrame({"foo": [1, 2, 3]})
        message = "df type is invalid, must be rf.DataFrame"
        with self.assertRaisesRegex(TypeError, message):
            df.append(1)

    def test_mismatched_columns(self):
        df1 = rf.DataFrame({"foo": [1, 2, 3]})
        df2 = rf.DataFrame({"bar": [4, 5, 6]})
        result = df1.append(df2)
        data = {"foo": [1, 2, 3, None, None, None], "bar": [None, None, None, 4, 5, 6]}
        expected = rf.DataFrame(data)
        self.assertEqual(result, expected)

    def test_matched_single_columns(self):
        df1 = rf.DataFrame({"foo": [1, 2, 3]})
        df2 = rf.DataFrame({"foo": [4, 5, 6]})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": [1, 2, 3, 4, 5, 6]})
        self.assertEqual(result, expected)

    def test_matched_double_columns(self):
        df1 = rf.DataFrame({"foo": [1, 2], "bar": ["a", "b"]})
        df2 = rf.DataFrame({"foo": [3, 4], "bar": ["c", "d"]})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": [1, 2, 3, 4], "bar": ["a", "b", "c", "d"]})
        self.assertEqual(result, expected)

    def test_matched_double_columns_out_of_order(self):
        df1 = rf.DataFrame({"foo": [1, 2], "bar": ["a", "b"]})
        df2 = rf.DataFrame({"bar": ["c", "d"], "foo": [3, 4]})
        result = df1.append(df2)
        expected = rf.DataFrame({"foo": [1, 2, 3, 4], "bar": ["a", "b", "c", "d"]})
        self.assertEqual(result, expected)
