<div align="center">
  <img src="https://kuzudb.com/img/kuzu-logo.png" height="100">
</div>

<br>

<p align="center">
  <a href="https://github.com/kuzudb/kuzu/actions">
    <img src="https://github.com/kuzudb/kuzu/actions/workflows/ci-workflow.yml/badge.svg?branch=master" alt="Github Actions Badge">
  </a>
  <a href="https://codecov.io/gh/kuzudb/kuzu" >
    <img src="https://codecov.io/github/kuzudb/kuzu/branch/master/graph/badge.svg?token=N1AT6H79LM"/>
  </a>
  <a href="https://join.slack.com/t/kuzudb/shared_invite/zt-1qgxnn8ed-9LL7rfKozijOtvw5HyWDlQ">
    <img src="https://img.shields.io/badge/Slack-chat%20with%20us-informational?logo=slack" alt="slack" />
  </a>
  <a href="https://twitter.com/kuzudb">
    <img src="https://img.shields.io/badge/follow-@kuzudb-1DA1F2?logo=twitter" alt="twitter">
  </a>
</p>

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
To build from source code, Kùzu requires Cmake(>=3.11), Python 3, and a compiler that supports `C++20`.
- Perform a full clean build without tests and benchmark:
  - `make clean && make release`
- Perform a full clean build with tests and benchmark (optional):
  - `make clean && make all`
- Run tests (optional):
  - `make test && make pytest`

For development, use `make debug` to build a non-optimized debug version.
To build in parallel, pass `NUM_THREADS` as parameter, e.g., `make NUM_THREADS=8`.

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
We take `tinysnb` as an example graph, which is under `dataset/demo-db/csv` in our source code, and can be downloaded [here](https://github.com/kuzudb/kuzu/tree/master/dataset/demo-db/csv).

### CLI
#### Start CLI
```
./build/release/tools/shell/kuzu_shell "./testdb"
```
#### Schema Definition
```cypher
kuzu> CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name));
kuzu> CREATE REL TABLE Follows(FROM User TO User, since INT64);
```

#### Data Import
```cypher
kuzu> COPY User FROM "dataset/demo-db/csv/user.csv";
kuzu> COPY Follows FROM "dataset/demo-db/csv/follows.csv";
```

After creating node/rel tables, and importing csv files, you can now run queries!
```cypher
kuzu> MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, b.name, f.since;
```

### Python
```python
import kuzu

db = kuzu.Database('./testdb')
conn = kuzu.Connection(db)
conn.execute("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))")
conn.execute("CREATE REL TABLE Follows(FROM User TO User, since INT64)")
conn.execute('COPY User FROM "user.csv"')
conn.execute('COPY Follows FROM "follows.csv"')
# Run a query.
results = conn.execute('MATCH (u:User) RETURN COUNT(*);')
while results.hasNext():
  print(results.getNext())
# Run a query and get results as a pandas dataframe.
results = conn.execute('MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;').getAsDF()
print(results)
# Run a query and get results as an arrow table.
results = conn.execute('MATCH (u:User) RETURN u.name, u.age;').get_as_arrow(chunk_size=100)
print(results)
```

Refer to our [Data Import](https://kuzudb.com/docs/data-import) and [Cypher](https://kuzudb.com/docs/cypher) section for more information.

## Contributing
We welcome contributions to Kùzu. If you are interested in contributing to Kùzu, please read our [Contributing Guide](CONTRIBUTING.md).

## License
By contributing to Kùzu, you agree that your contributions will be licensed under the [MIT License](LICENSE).

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
You can contact us at [contact@kuzudb.com](mailto:contact@kuzudb.com) or [join our Slack workspace](https://join.slack.com/t/kuzudb/shared_invite/zt-1n67h736q-E3AFGSI4w~ljlFMYr3_Sjg).
