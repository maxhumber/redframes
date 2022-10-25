import unittest
from pathlib import Path
from shutil import rmtree as delete
from tempfile import mkdtemp as make_temp_dir


class TestReadme(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempdir = make_temp_dir()
        self.path = str(Path(tempdir) / "bears.csv")

    def tearDown(self):
        delete(self.tempdir)

    def test_quick_start(self):
        import redframes as rf

        df = rf.DataFrame(
            {
                "bear": [
                    "Brown bear",
                    "Polar bear",
                    "Asian black bear",
                    "American black bear",
                    "Sun bear",
                    "Sloth bear",
                    "Spectacled bear",
                    "Giant panda",
                ],
                "genus": [
                    "Ursus",
                    "Ursus",
                    "Ursus",
                    "Ursus",
                    "Helarctos",
                    "Melursus",
                    "Tremarctos",
                    "Ailuropoda",
                ],
                "weight (male, lbs)": [
                    "300-860",
                    "880-1320",
                    "220-440",
                    "125-500",
                    "60-150",
                    "175-310",
                    "220-340",
                    "190-275",
                ],
                "weight (female, lbs)": [
                    "205-455",
                    "330-550",
                    "110-275",
                    "90-300",
                    "45-90",
                    "120-210",
                    "140-180",
                    "155-220",
                ],
            }
        )

        # | bear                | genus      | weight (male, lbs)   | weight (female, lbs)   |
        # |:--------------------|:-----------|:---------------------|:-----------------------|
        # | Brown bear          | Ursus      | 300-860              | 205-455                |
        # | Polar bear          | Ursus      | 880-1320             | 330-550                |
        # | Asian black bear    | Ursus      | 220-440              | 110-275                |
        # | American black bear | Ursus      | 125-500              | 90-300                 |
        # | Sun bear            | Helarctos  | 60-150               | 45-90                  |
        # | Sloth bear          | Melursus   | 175-310              | 120-210                |
        # | Spectacled bear     | Tremarctos | 220-340              | 140-180                |
        # | Giant panda         | Ailuropoda | 190-275              | 155-220                |

        (
            df.rename({"weight (male, lbs)": "male", "weight (female, lbs)": "female"})
            .gather(["male", "female"], into=("sex", "weight"))
            .split("weight", into=["min", "max"], sep="-")
            .gather(["min", "max"], into=("stat", "weight"))
            .mutate({"weight": lambda row: float(row["weight"])})
            .group(["genus", "sex"])
            .rollup({"weight": ("weight", rf.stat.mean)})
            .spread("sex", using="weight")
            .mutate({"dimorphism": lambda row: round(row["male"] / row["female"], 2)})
            .drop(["male", "female"])
            .sort("dimorphism", descending=True)
        )

        # | genus      |   dimorphism |
        # |:-----------|-------------:|
        # | Ursus      |         2.01 |
        # | Tremarctos |         1.75 |
        # | Helarctos  |         1.56 |
        # | Melursus   |         1.47 |
        # | Ailuropoda |         1.24 |

        self.assertTrue(True)

    def test_pandas_comparison(self):
        import pandas as pd

        df = pd.DataFrame(
            {
                "bear": [
                    "Brown bear",
                    "Polar bear",
                    "Asian black bear",
                    "American black bear",
                    "Sun bear",
                    "Sloth bear",
                    "Spectacled bear",
                    "Giant panda",
                ],
                "genus": [
                    "Ursus",
                    "Ursus",
                    "Ursus",
                    "Ursus",
                    "Helarctos",
                    "Melursus",
                    "Tremarctos",
                    "Ailuropoda",
                ],
                "weight (male, lbs)": [
                    "300-860",
                    "880-1320",
                    "220-440",
                    "125-500",
                    "60-150",
                    "175-310",
                    "220-340",
                    "190-275",
                ],
                "weight (female, lbs)": [
                    "205-455",
                    "330-550",
                    "110-275",
                    "90-300",
                    "45-90",
                    "120-210",
                    "140-180",
                    "155-220",
                ],
            }
        )

        df = df.rename(
            columns={"weight (male, lbs)": "male", "weight (female, lbs)": "female"}
        )
        df = pd.melt(
            df,
            id_vars=["bear", "genus"],
            value_vars=["male", "female"],
            var_name="sex",
            value_name="weight",
        )
        df[["min", "max"]] = df["weight"].str.split("-", expand=True)
        df = df.drop("weight", axis=1)
        df = pd.melt(
            df,
            id_vars=["bear", "genus", "sex"],
            value_vars=["min", "max"],
            var_name="stat",
            value_name="weight",
        )
        df["weight"] = df["weight"].astype("float")
        df = df.groupby(["genus", "sex"])["weight"].mean()
        df = df.reset_index()
        df = pd.pivot_table(df, index=["genus"], columns=["sex"], values="weight")
        df = df.reset_index()
        df = df.rename_axis(None, axis=1)
        df["dimorphism"] = round(df["male"] / df["female"], 2)
        df = df.drop(["female", "male"], axis=1)
        df = df.sort_values("dimorphism", ascending=False)
        df = df.reset_index(drop=True)

        self.assertTrue(True)

    def test_io(self):
        import redframes as rf

        df = rf.DataFrame({"foo": [1, 2], "bar": ["A", "B"]})

        # save .csv
        rf.save(df, self.path)

        # load .csv
        df = rf.load(self.path)

        # convert redframes → pandas
        pandas_df = rf.unwrap(df)

        # convert pandas → redframes
        df = rf.wrap(pandas_df)

        self.assertTrue(True)

    def test_properties(self):
        import redframes as rf

        df = rf.DataFrame({"genus": [1]})

        df["genus"]
        # ['Ursus', 'Ursus', 'Ursus', 'Ursus', 'Helarctos', 'Melursus', 'Tremarctos', 'Ailuropoda']

        df.columns
        # ['bear', 'genus', 'weight (male, lbs)', 'weight (female, lbs)']

        df.dimensions
        # {'rows': 8, 'columns': 4}

        df.empty
        # False

        df.memory
        # '2 KB'

        df.types
        # {'bear': object, 'genus': object, 'weight (male, lbs)': object, 'weight (female, lbs)': object}

        self.assertTrue(True)

    def test_matplotlib(self):
        import matplotlib.pyplot as plt

        import redframes as rf

        football = rf.DataFrame(
            {
                "position": ["TE", "K", "RB", "WR", "QB"],
                "avp": [116.98, 131.15, 180, 222.22, 272.91],
            }
        )

        df = football.mutate(
            {"color": lambda row: row["position"] in ["WR", "RB"]}
        ).replace({"color": {False: "orange", True: "red"}})

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
