<div align="center">
  <img src="/logo/kuzu-logo.png" height="100">
</div>

# Kùzu
Kùzu is an in-process property graph database management system (GDBMS) built for query speed and scalability. Kùzu is optimized for handling complex join-heavy analytical workloads on very large graph databases, with the following core feature set:

- Flexible Property Graph Data Model and Cypher query language
- Embeddable, serverless integration into applications 
- Columnar disk-based storage
- Columnar sparse row-based (CSR) adjacency list/join indices
- Vectorized and factorized query processor
- Novel and very fast join algorithms
- Multi-core query parallelism
- Serializable ACID transactions

Kùzu is being actively developed at University of Waterloo as a feature-rich and usable GDBMS. Kùzu is available under a permissible license. So try it out and help us make it better! We welcome your feedback and feature requests.

## Build
To build from source code, Kùzu requires Cmake(>=3.11), Python 3, [Apache Arrow](https://arrow.apache.org/), [xsimd](https://xsimd.readthedocs.io/en/latest/), and a compiler that supports `C++20`.  
- Perform a full clean build
  - `make clean && make`
- Run tests (optional)
  - `make test`

After build, our CLI binary `kuzu_shell` is available under the directory `build/release/tools/shell/`.

## Installation
### Precompiled binary
Precompiled binary of our latest release can be downloaded [here](https://github.com/kuzudb/kuzu/releases/tag/0.0.1).  
### Python package
Our Python package can be directly install through pip.
```
pip install kuzu
```


For more installation and usage instructions, please refer to [our website](https://kuzudb.com/).

## Example
We take `tinysnb` as an example graph, which is under `dataset/tinysnb` in our source code, and can be downloaded [here](https://github.com/kuzudb/kuzu/tree/master/dataset/tinysnb).

### CLI
#### Start CLI
```
./build/release/tools/shell/kuzu_shell -i "./test.db"
```
#### Schema Definition
```cypher
kuzu> CREATE NODE TABLE person (ID INt64, fName STRING, gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));
kuzu> CREATE REL TABLE knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, comments STRING[], MANY_MANY);
```

#### Data Import
```cypher
kuzu> COPY person FROM "dataset/tinysnb/vPerson.csv" (HEADER=true);
kuzu> COPY knows FROM "dataset/tinysnb/eKnows.csv";
```

After creating node/rel tables, and importing csv files, you can now run queries!
```cypher
kuzu> MATCH (b:person)<-[e1:knows]-(a:person)-[e2:knows]->(c:person) RETURN COUNT(*);
```

### Python
```python
import kuzu

db = kuzu.database('./testpy')
conn = kuzu.connection(db)
conn.execute("CREATE NODE TABLE person (ID INt64, fName STRING, gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));")
conn.execute("CREATE REL TABLE knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, comments STRING[], MANY_MANY);")
conn.execute("COPY person FROM 'dataset/tinysnb/vPerson.csv' (HEADER=true);")
conn.execute("COPY knows FROM 'dataset/tinysnb/eKnows.csv';")
result = conn.execute("MATCH (b:person)<-[e1:knows]-(a:person)-[e2:knows]->(c:person) RETURN COUNT(*)")
while result.hasNext():
  print(result.getNext())
```

Refer to our [Data Import](https://kuzudb.com/docs/data-import) and [Cypher](https://kuzudb.com/docs/cypher) section for more information.

## Code of Conduct
We are developing Kùzu in a publicly open GitHub repo and encourage everyone 
to comment and discuss topics related to Kùzu. However, we ask everyone to be
very respectful and polite in discussions and avoid any hurtful language in any form.
Kùzu is being developed in Canada, a land that is proud in their high standards
for politeness and where a person you bumped into on the road will likely
apologize to you even if it was your fault: so please be 
kind and respectful to everyone. 
If you prefer not to discuss something through open issues and discussions, 
you can always reach us through [email](mailto:contact@kuzudb.com).

## Citing Kùzu
If you are a researcher and use Kùzu in your work, we encourage you to cite our work.
You can use the following BibTeX citation:
```
@inproceedings{kuzu:cidr,
  author =  {Xiyang Feng and
             Guodong Jin and
             Ziyi Chen and
             Chang Liu and
             Semih Saliho\u{g}lu},
  title={K\`uzu Graph Database Management System},
  booktitle={CIDR},
  year={2023}
}
@misc{kuzu-github,
  author =  {Xiyang Feng and
             Guodong Jin and
             Ziyi Chen and
             Chang Liu and
             Semih Saliho\u{g}lu},
  title = {{K\`uzu Database Management System Source Code}},
  howpublished = {\url{https://github.com/kuzudb/kuzu}},
  month        = nov,
  year         = 2022
}
```

## Contact Us
You can contact us at [contact@kuzudb.com](mailto:contact@kuzudb.com).
