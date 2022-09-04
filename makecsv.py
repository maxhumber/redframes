import pandas as pd
import datetime
import random
import string

def random_date():
    startdate = datetime.date(2022, 9, 4)
    date = startdate + datetime.timedelta(random.randint(1,365))
    return date

def random_string(length=1):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def random_float():
    return random.randrange(-10_000, 10_000_000) / 100

def random_int():
    return random.randint(0, 10)

def random_bool():
    return bool(random.randint(0, 1))

N = 100_000

df = pd.DataFrame({
    "timestamp": [random_date() for _ in range(N)],
    "name": [random_string(5) for _ in range(N)],
    "wealth": [random_float() for _ in range(N)],
    "tier": [random_int() for _ in range(N)],
    "car": [random_bool() for _ in range(N)]
})

df

for col in df.columns:
    df.loc[df.sample(frac=0.02).index, col] = pd.np.nan

df.to_csv("test.csv", index=False)

df.sample(100)
