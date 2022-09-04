import csv
import sqlite3
import os
import fnmatch


import sqlite3
import csv

con = sqlite3.connect(":memory:")
cur = con.cursor()
with open("example.csv", 'r') as f:
    dr = csv.DictReader(f)
    print(dr)
    TABLE_NAME = "lhs"
    Cols = dr.fieldnames
    numCols = len(Cols)
    to_db = [tuple(i.values()) for i in dr]
    print(TABLE_NAME)
    # use your column names here
    ColString = ','.join(Cols)
    QuestionMarks = ["?"] * numCols
    ToAdd = ','.join(QuestionMarks)
    cur.execute(f"CREATE TABLE {TABLE_NAME} ({ColString});")
    cur.executemany(
        f"INSERT INTO {TABLE_NAME} ({ColString}) VALUES ({ToAdd});", to_db)
    con.commit()
# con.close()
print("Execution Complete!")

results = cur.execute('select * from lhs').fetchall()





import sqlite3

# Create a database in RAM
db = sqlite3.connect(':memory:')
c = db.cursor()
c.execute('create table t (id,a,b,c);')
db.commit()
d = {0:[1.0,2.0,3.0]}
c.execute("SELECT * FROM sqlite_master WHERE type='table'").fetchall()


db2 = sqlite3.connect(':memory:')
c2 = db2.cursor()
c2.execute('create table t2 (id,a,b,c);')
db2.commit()
c2.execute("SELECT * FROM sqlite_master WHERE type='table'").fetchall()

db = sqlite3.connect('file:my_db?mode=memory&cache=shared', uri=True)
c = db.cursor()

import itertools as it
c.execute('insert into t values (?,?,?,?)',[i for i in it.chain(d.keys(),*d.values())])
db.commit()
results = c.execute('select * from t where id = 0').fetchall()


##### LEFT JOINING TWO INMEMORY DATABASES

import sqlite3

c1 = sqlite3.connect("file:db1?mode=memory&cache=shared", uri=True)
c1.execute('CREATE TABLE t1 (bar, baz)')
c1.execute("INSERT INTO t1 VALUES ('spam', 'ham')")
c1.commit()
c1.execute("select * from main.t1").fetchall()

c2 = sqlite3.connect("file:db2?mode=memory&cache=shared", uri=True)
c2.execute('CREATE TABLE t2 (bar, jaz)')
c2.execute("INSERT INTO t2 VALUES ('spam', 'fam')")
c2.commit()

c3 = sqlite3.connect("file:db3?mode=memory&cache=shared", uri=True)
c3.execute('CREATE TABLE t3 (bar, jaz)')
c3.execute("INSERT INTO t3 VALUES ('lol', 'lol')")
c3.commit()

c2.execute("ATTACH DATABASE 'file:db1?mode=memory&cache=shared' AS db1")
c2.execute("select * from t2 left join db1.t1 on t2.bar = db1.t1.bar").fetchall()

c2.execute("SELECT * FROM sqlite_master WHERE type='table'").fetchall()


### HYDRATE

import string
import random

def get_random_string(length=1):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

db = sqlite3.connect("file:db3?mode=memory&cache=shared", uri=True)
db.execute('CREATE TABLE name (name, number)')

db.execute('''
    CREATE TABLE IF NOT EXISTS random_table(
        id INTEGER PRIMARY KEY,
        name TEXT,
        number INTEGER
    )
''')

i = 0
vals = []
sql = "INSERT INTO random_table (name, number) VALUES (?, ?)"
while i < 1_000_000:
    vals.append((get_random_string(8), random.randint(1900, 2000)))
    i += 1
    if i % 100 == 0:
        db.executemany(sql, vals)
        vals.clear()

db.execute("select * from random_table limit 100").fetchall()

import re
def contains(value, pattern):
    c_pattern = re.compile(r"\b" + pattern.lower() + r"\b")
    return c_pattern.search(value) is not None

contains("This is a test message", "not")

db.create_function("REGEX_CONTAINS", 2, contains)

db.execute('SELECT * FROM random_table WHERE REGEX_CONTAINS(name, ?)', ('vro',)).fetchall()

db.create_function("MULTI5", 1, lambda x: x * 5)

%%timeit
db.execute('SELECT number, MULTI5(number) FROM random_table').fetchall()


numbers = [random.randint(1900, 2000) for _ in range(1_000_000)]

%%timeit
list(map(lambda x: x*5, numbers))

import pandas

df = pd.DataFrame()


import sqlite3
import re


def functionRegex(value, pattern):
    c_pattern = re.compile(r"\b" + pattern.lower() + r"\b")
    return c_pattern.search(value) is not None


connection = sqlite3.connect(':memory:')
cur = connection.cursor()
cur.execute('CREATE TABLE tweet(msg TEXT)')
cur.execute('INSERT INTO tweet VALUES("This is a test message")')
cur.execute('INSERT INTO tweet VALUES("Another message")')

connection.create_function("REGEXP", 2, functionRegex)

print(cur.execute('SELECT * FROM tweet WHERE REGEXP(msg, ?)', ('test',)).fetchall())
print(cur.execute('SELECT * FROM tweet WHERE REGEXP(msg, ?)', ('message',)).fetchall())
print(cur.execute('SELECT * FROM tweet WHERE ? REGEXP msg', ('message',)).fetchall())



>>> c2 = sqlite3.connect("file::memory:?cache=shared", uri=True)
>>> list(c2.execute('SELECT * FROM foo'))
[(u'spam', u'ham')]
>>> c3 = sqlite3.connect('/tmp/sqlite3.db', uri=True)
>>> c3.execute("ATTACH DATABASE 'file::memory:?cache=shared' AS inmem")
<sqlite3.Cursor object at 0x1068395e0>
>>> list(c3.execute('SELECT * FROM inmem.foo'))
[(u'spam', u'ham')]


    rc = sqlite3_open("file:memdb1?mode=memory&cache=shared", &db);

Or,

    ;






# Creates or opens a file called mydb with a SQLite3 DB
# db = sqlite3.connect('db.sqlite3')

##########
# CREATE #
##########
cursor = db.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY,
        name TEXT,
        phone TEXT,
        email TEXT,
        password TEXT
    )
''')
db.commit()
db


##########
# INSERT #
##########
'''
If you need values from Python variables it is recommended to use the "?" placeholder.
Never use string operations or concatenation to make your queries because is very insecure.
'''
cursor = db.cursor()
name = 'Andres'
phone = '3366858'
email = 'user@example.com'
password = '12345'
cursor.execute('''INSERT INTO users(name, phone, email, password)
                  VALUES(?,?,?,?)''', (name,phone, email, password))
db.commit()
'''
The values of the Python variables are passed inside a tuple.
Another way to do this is passing a dictionary using the ":keyname" placeholder:
'''
cursor = db.cursor()
cursor.execute('''INSERT INTO users(name, phone, email, password)
                  VALUES(:name,:phone, :email, :password)''',
                  {'name':name, 'phone':phone, 'email':email, 'password':password})
db.commit()

# If you need to insert several users use executemany and a list with the tuples:
users = [('a','1', 'a@b.com', 'a1'),
         ('b','2', 'b@b.com', 'b1'),
         ('c','3', 'c@b.com', 'c1'),
         ('c','3', 'c@b.com', 'c1')]
cursor.executemany(''' INSERT INTO users(name, phone, email, password) VALUES(?,?,?,?)''', users)
db.commit()


# ????
# If you need to get the id of the row you just inserted use lastrowid:
id = cursor.lastrowid
print('Last row id: %d' % id)


##########
# SELECT #
##########
# To retrieve data, execute the query against the cursor object
# and then use fetchone() to retrieve a single row or fetchall() to retrieve all the rows.

cursor.execute('''SELECT name, email, phone FROM users''')
user1 = cursor.fetchone() #retrieve the first row
print(user1[0])
all_rows = cursor.fetchall()
for row in all_rows:
    # row[0] returns the first column in the query (name), row[1] returns email column.
    print('{0} : {1}, {2}'.format(row[0], row[1], row[2]))
# The cursor object works as an iterator, invoking fetchall() automatically:
cursor.execute('''SELECT name, email, phone FROM users''')
for row in cursor:
    print('{0} : {1}, {2}'.format(row[0], row[1], row[2]))

# To retrive data with conditions, use again the "?" placeholder:
user_id = 3
cursor.execute('''SELECT name, email, phone FROM users WHERE id=?''', (user_id,))
user = cursor.fetchone()
db.commit()

##########
# UPDATE #
##########
# The procedure to update data is the same as inserting data:
newphone = '3113093164'
userid = 1
cursor.execute('''UPDATE users SET phone = ? WHERE id = ? ''', (newphone, userid))
db.commit()

##########
# DELETE #
##########
# The procedure to delete data is the same as inserting data:
delete_userid = 2
cursor.execute('''DELETE FROM users WHERE id = ? ''', (delete_userid,))
db.commit()





### About commit() and rollback():
'''
Using SQLite Transactions:
Transactions are an useful property of database systems.
It ensures the atomicity of the Database.
Use commit to save the changes.
Or rollback to roll back any change to the database since the last call to commit:
'''
cursor.execute('''UPDATE users SET phone = ? WHERE id = ? ''', (newphone, userid))
# The user's phone is not updated
db.rollback()

'''
Please remember to always call commit to save the changes.
If you close the connection using close or the connection to the file is lost
(maybe the program finishes unexpectedly), not committed changes will be lost.
'''



### Exception Handling:
try:
    db = sqlite3.connect('db.sqlite3')
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS
                      users(id INTEGER PRIMARY KEY, name TEXT, phone TEXT, email TEXT, password TEXT)''')
    db.commit()
except Exception as e:
    # This is called a catch-all clause.
    # This is used here only as an example.
    # In a real application you should catch a specific exception such as IntegrityError or DatabaseError

    # Roll back any change if something goes wrong
    db.rollback()
    raise e
finally:
    db.close()

### SQLite Row Factory and Data Types
'''
The following table shows the relation between SQLite datatypes and Python datatypes:
    None type is converted to NULL
    int type is converted to INTEGER
    float type is converted to REAL
    str type is converted to TEXT
    bytes type is converted to BLOB
'''

# The row factory class sqlite3.Row is used to access the columns of a query by name instead of by index:
db = sqlite3.connect('db.sqlite3')
db.row_factory = sqlite3.Row
cursor = db.cursor()
cursor.execute('''SELECT name, email, phone FROM users''')
for row in cursor:
    # row['name'] returns the name column in the query, row['email'] returns email column.
    print('{0} -> {1}, {2}'.format(row['name'], row['email'], row['phone']))
db.close()



########
# DROP #
########
db = sqlite3.connect('db.sqlite3')
cursor = db.cursor()
cursor.execute('''DROP TABLE users''')
db.commit()

# When we are done working with the DB we need to close the connection:
db.close()
