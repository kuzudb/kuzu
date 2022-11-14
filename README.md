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

## Installation
Please refer to [our website](https://kuzudb.com/) for installation and usage instructions.


## Development
For development, Kùzu requires Bazel, Python 3, and a compiler that supports `C++20`. 
### Bazel configuration
- Create bazel configuration file with `touch .bazelrc`
- Add bazel configuration `echo build --cxxopt="-std=c++2a" --cxxopt='-O3' > .bazelrc`
### Bazel build

- To do a full clean build
    - `bazel clean --expunge`
    - `bazel build //...:all`
- To test
    - `bazel test //...:all`
