### todo

simpledf
tidydf
easydf
gooddf
tibble
from tibble import Tibble
framer import DataFrame
from pandaz import DataFrame

from bearcats import DataFrame
import redpandas as rd
redas
[Re]ctangular [Da]taframe[s]
redframes
rectangular data frames
import redframes as rf.DataFrame()
redf
from redf import DataFrame
import redf as rd
from redframe import DataFrame


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
    - ✅ reindex
    - ✅ dropna
    - fillna
    - fill (ffill/bfill)
    - ❌ shift
    - ❌ replace
    - ❌ replacena
    - ❌ slice
- column operations
    - ✅ select
    - ✅ rename
    - ✅ mutate
    - ✅ join
    - ✅ separate
    - ✅ combine
    - drop (columns)
    - ❌ retype / to_datetime / to_numeric
- other
    - ✅ gather
    - ✅ spread
    - complete
    - group
    - ungroup
    - dump
    - to_dict
    - convert_to_pandas
    - ❌ iterrows
- aggregation operations
    - reduce
    - summarise
    - count / tally
