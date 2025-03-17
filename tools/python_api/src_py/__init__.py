"""
# Kuzu Python API bindings.

This package provides a Python API for Kuzu graph database management system.

To install the package, run:
```
python3 -m pip install kuzu
```

Example usage:
```python
import kuzu

db = kuzu.Database("./test")
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
results = conn.execute("MATCH (u:User) RETURN u.name, u.age;")
while results.has_next():
    print(results.get_next())
```

The dataset used in this example can be found [here](https://github.com/kuzudb/kuzu/tree/master/dataset/demo-db/csv).

"""

from __future__ import annotations

import os
import sys

# Set RTLD_GLOBAL and RTLD_LAZY flags on Linux to fix the issue with loading
# extensions
if sys.platform == "linux":
    original_dlopen_flags = sys.getdlopenflags()
    sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

from .async_connection import AsyncConnection
from .connection import Connection
from .database import Database
from .prepared_statement import PreparedStatement
from .query_result import QueryResult
from .types import Type


def __getattr__(name: str) -> str | int:
    if name in ("version", "__version__"):
        return Database.get_version()
    elif name == "storage_version":
        return Database.get_storage_version()
    else:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)


# Restore the original dlopen flags
if sys.platform == "linux":
    sys.setdlopenflags(original_dlopen_flags)

__all__ = [
    "AsyncConnection",
    "Connection",
    "Database",
    "PreparedStatement",
    "QueryResult",
    "Type",
    "__version__",
    "storage_version",
    "version",
]
