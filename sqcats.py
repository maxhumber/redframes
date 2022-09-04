import sqlite3
import uuid
import pandas as pd

class DataFrame:
    def __init__(self, data):
        self._id = id = str(uuid.uuid4()).replace("-", "")
        self._uri = uri = f'file:{id}?mode=memory&cache=shared'
        self._db = db = sqlite3.connect(uri, uri=True, detect_types=sqlite3.PARSE_DECLTYPES)
        df = pd.read_csv(data)
        df.to_sql('df', con=db, if_exists='replace')

    def __getitem__(self, key):
        self._db.row_factory = lambda _, row: row[0]
        values = self._db.execute(f"SELECT {key} FROM df".format(key)).fetchall()
        self._db.row_factory = None
        return values

df = DataFrame("test.csv")
df._id
df._uri

%%timeit
values = df["timestamp"]

df._db.execute("select timestamp from df limit 100").fetchall()

df2 = pd.read_csv("test.csv")

%%timeit
values = df2["timestamp"].values.tolist()

values






#
