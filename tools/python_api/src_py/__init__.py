"""
# Kùzu Python API bindings

This package provides a Python API for Kùzu graph database management system.

To install the package, run:
```
python3 -m pip install kuzu
```

Example usage:
```python
import kuzu

db = kuzu.Database('./test')
conn = kuzu.Connection(db)

# Define the schema
conn.execute("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))")
conn.execute("CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))")
conn.execute("CREATE REL TABLE Follows(FROM User TO User, since INT64)")
conn.execute("CREATE REL TABLE LivesIn(FROM User TO City)")

# Load some data
conn.execute('COPY User FROM "user.csv"')
conn.execute('COPY City FROM "city.csv"')
conn.execute('COPY Follows FROM "follows.csv"')
conn.execute('COPY LivesIn FROM "lives-in.csv"')

# Query the data
results = conn.execute('MATCH (u:User) RETURN u.name, u.age;')
while results.has_next():
    print(results.get_next())
```

The dataset used in this example can be found [here](https://github.com/kuzudb/kuzu/tree/master/dataset/demo-db/csv).

"""

import sys
import os

# Set RTLD_GLOBAL and RTLD_LAZY flags on Linux to fix the issue with loading
# extensions
if sys.platform == "linux":
    original_dlopen_flags = sys.getdlopenflags()
    sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

from .database import *
from .connection import *
from .query_result import *
from .types import *

def __getattr__(name):
    if name == "version":
        return Database.get_version()
    elif name == "storage_version":
        return Database.get_storage_version()
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    

# Restore the original dlopen flags
if sys.platform == "linux":
    sys.setdlopenflags(original_dlopen_flags)
