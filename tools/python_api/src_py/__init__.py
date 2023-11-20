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

from .database import *
from .connection import *
from .query_result import *
from .types import *
