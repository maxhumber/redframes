### todo

from bearcats import DataFrame
from redframes import RedFrame

- add datasets
- make sure it works with fantasy
- machine learning
- visualization

- intrinsic
    - ✅ __init__
    - ✅ __getitem__
    - ✅ __repr__
    - ✅ _repr_html_
- properties
    - ✅ columns
    - ✅ dimensions
    - ✅ values
    - ✅ types
    - ✅ empty
- row operations
    - ✅ head
    - ✅ tail
    - ✅ sample
    - ✅ filter
    - ✅ sort
    - ✅ dedupe (distinct, deduplicate, drop_duplicates)
    - ✅ append
    - ✅ dropna
    - ✅ fillna (ffill/bfill)
    - ❌ shift
    - ❌ replacena
    - ❌ slice (rows)
- column operations
    - ✅ select
    - ✅ rename
    - ✅ mutate
    - ✅ join
    - ✅ separate
    - ✅ combine
    - drop (columns)
- other
    - ✅ gather
    - ✅ spread
    - ✅ complete
    - summarise
    - count / tally
    - dump
    - to_dict
    - convert_to_pandas
