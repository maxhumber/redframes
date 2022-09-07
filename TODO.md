### todo

- intrinsic
    - ✅ `__init__`
    - ✅ `__getitem__`
    - ✅ `_repr_html_`
    - ✅ `__repr__`
    - ✅ `__eq__`
- properties
    - ✅ shape
    - ✅ types
    - ✅ columns
    - ✅ rows
    - ✅ empty
- row
    - ✅ take (any number of rows from head or tail)
    - 🫥 slice (any number of rows from a start index to an end)
    - ✅ sample (any rows by count or percent)
    - 🫥 shuffle (all rows) 
    - ✅ sort (rows by column)
    - ✅ filter (rows by column condition)
    - ✅ dedupe (columns by keeping only those that are unique)
    - ✅ sanitize (columns by droping rows that contain NA/Null/None)
    - ✅ fill (with down, up, and constant options)
    - 🫥 replace (values in columns with new row options)
- column
    - ✅ rename (columns with dictionary)
    - ✅ select (columns to keep)
    - 🫥 discard (unneeded columns)
    - ✅ mutate (new columns with functions)
    - split (a column into other columns)
    - combine (multiple columns into a single column)
- other
    - append (another DataFrame)
    - join (another DataFrame)
    - gather
    - spread
    - complete
    - aggregate
    - ⚠️ export/save/write/dump

- groupby
    - 

unique values
value counts
tally?


----

- docstrings
- add datasets
- make sure it works with fantasy
- machine learning
- visualization
- cheat sheet
- 10 minutes
- readme

to_pandas
from_pandas
convert

SHOULD I INSTANTIATE OFF PANDAS? CONVERT FUNCTION OUTSIDE?
WHAT ABOUT SQL?
Instaniate from rows/columns?
