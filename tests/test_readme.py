import unittest
from pathlib import Path
from shutil import rmtree as delete
from tempfile import mkdtemp as make_temp_dir


class TestReadme(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempdir = make_temp_dir()
        self.path = str(Path(tempdir) / "example.csv")

    def tearDown(self):
        delete(self.tempdir)

    def test_quick_start(self):
        import redframes as rf

        df = rf.DataFrame(
            {
                "foo": ["A", "A", "B", None, "B", "A", "A", "C"],
                "bar": [1, 4, 2, -4, 5, 6, 6, -2],
                "baz": [0.99, None, 0.25, 0.75, 0.66, 0.47, 0.48, None],
            }
        )

        df["foo"]
        # ['A', 'A', 'B', None, 'B', 'A', 'A', 'C']
        df.columns
        # ['foo', 'bar', 'baz']
        df.dimensions
        # {'rows': 8, 'columns': 3}
        df.empty
        # False
        df.types
        # {'foo': object, 'bar': int, 'baz': float}

        (
            df.mutate({"bar100": lambda row: row["bar"] * 100})
            .select(["foo", "baz", "bar100"])
            .filter(lambda row: (row["foo"].isin(["A", "B"])) & (row["bar100"] > 0))
            .denix("baz")
            .group("foo")
            .rollup(
                {"bar_mean": ("bar100", rf.stat.mean), "baz_sum": ("baz", rf.stat.sum)}
            )
            .gather(["bar_mean", "baz_sum"])
            .sort("value")
        )

        self.assertTrue(True)

    def test_io(self):
        import pandas as pd

        import redframes as rf

        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})

        # save/load
        rf.save(df, self.path)
        df = rf.load(self.path)

        # to/from pandas
        pandf = rf.unwrap(df)
        reddf = rf.wrap(pandf)

        self.assertTrue(True)

    def test_matplotlib(self):
        import matplotlib.pyplot as plt

        import redframes as rf

        df = rf.DataFrame(
            {
                "position": ["TE", "K", "RB", "WR", "QB"],
                "avp": [116.98, 131.15, 180, 222.22, 272.91],
            }
        )

        df = df.mutate({"color": lambda row: row["position"] in ["WR", "RB"]}).replace(
            {"color": {False: "orange", True: "red"}}
        )

        plt.barh(df["position"], df["avp"], color=df["color"])

        self.assertTrue(True)

    def test_sklearn(self):
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split

        import redframes as rf

        df = rf.DataFrame(
            {
                "touchdowns": [15, 19, 5, 7, 9, 10, 12, 22, 16, 10],
                "age": [21, 22, 21, 24, 26, 28, 30, 35, 28, 21],
                "mvp": [1, 1, 0, 0, 0, 0, 0, 1, 0, 0],
            }
        )

        target = "touchdowns"
        y = df[target]
        X = df.drop(target)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=1
        )

        model = LinearRegression()
        model.fit(X_train, y_train)
        model.score(X_test, y_test)
        # 0.5083194901655527

        # print(X_train.take(1))
        # rf.DataFrame({'age': [21], 'mvp': [0]})

        X_new = rf.DataFrame({"age": [22], "mvp": [1]})
        model.predict(X_new)
        # array([19.])

        self.assertTrue(True)
