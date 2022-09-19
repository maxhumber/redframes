import unittest
from pathlib import Path
from shutil import rmtree as delete
from tempfile import mkdtemp as make_temp_dir

import pandas as pd

import redframes as rf


class TestIO(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempdir = make_temp_dir()
        self.df = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.pdf = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.path = str(Path(tempdir) / "example.csv")

    def tearDown(self):
        delete(self.tempdir)

    def test_load_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            rf.load("test_missing_file.csv")

    def test_load_bad_format(self):
        with self.assertRaisesRegex(TypeError, "must end in .csv"):
            rf.load("test_bad_file_format.json")

    def test_save_bad_path_format(self):
        with self.assertRaisesRegex(TypeError, "must end in .csv"):
            rf.save(self.df, "example.json")

    def test_save_bad_type(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.save(1, "example.json")

    def test_unwrap_bad_type(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.unwrap(1)

    def test_wrap_bad_type(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.wrap(1)

    def test_unwrap_wrong_direction(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.unwrap(self.pdf)

    def test_wrap_wrong_direction(self):
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            rf.wrap(self.df)

    def test_unwrap_no_side_effect(self):
        pdf = rf.unwrap(self.df)
        pdf.columns = ["oof", "rab"]
        expected = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.assertEqual(self.df, expected)

    def test_wrap_no_side_effect(self):
        df = rf.wrap(self.pdf)
        df = df.rename({"foo": "oof"})
        expected = pd.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.assertTrue(self.pdf.equals(expected))

    def test_round_trip_save_load(self):
        rf.save(self.df, self.path)
        result = rf.load(self.path)
        expected = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.assertEqual(result, expected)

    def test_round_trip_unwrap_wrap(self):
        pdf = rf.unwrap(self.df)
        result = rf.wrap(pdf)
        expected = rf.DataFrame({"foo": [1, 2], "bar": [3, 4]})
        self.assertEqual(result, expected)
