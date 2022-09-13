import unittest
from pathlib import Path
from shutil import rmtree as delete
from tempfile import mkdtemp as make_temp_dir

import pandas as pd

import redframes as rf


class TestIOLoad(unittest.TestCase):
    def setUp(self):
        self.tempdir = make_temp_dir()

    def tearDown(self):
        delete(self.tempdir)

    def test_simple(self):
        path = str(Path(self.tempdir) / "test_load_simple.csv")
        df = pd.DataFrame({"foo": [1, 2, 3], "bar": ["a", "b", "c"]})
        df.to_csv(path, index=False)
        df = rf.load(path)
        self.assertIsNotNone(df)

    def test_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            rf.load("test_missing_file.csv")

    def test_bad_file_format(self):
        message = "file at path is invalid, must be a csv"
        with self.assertRaisesRegex(TypeError, message):
            rf.load("test_bad_file_format.json")
