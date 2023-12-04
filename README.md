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
  <a href="https://join.slack.com/t/kuzudb/shared_invite/zt-1w0thj6s7-0bLaU8Sb~4fDMKJ~oejG_g">
    <img src="https://img.shields.io/badge/Slack-chat%20with%20us-informational?logo=slack" alt="slack" />
  </a>
  <a href="https://twitter.com/kuzudb">
    <img src="https://img.shields.io/badge/follow-@kuzudb-1DA1F2?logo=twitter" alt="twitter">
  </a>
</p>

# Kùzu
Kùzu is an embedded graph database built for query speed and scalability. Kùzu is optimized for handling complex join-heavy analytical workloads on very large databases, with the following core feature set:

- Flexible Property Graph Data Model and Cypher query language
- Embeddable, serverless integration into applications 
- Columnar disk-based storage
- Columnar sparse row-based (CSR) adjacency list/join indices
- Vectorized and factorized query processor
- Novel and very fast join algorithms
- Multi-core query parallelism
- Serializable ACID transactions

Kùzu is being actively developed at University of Waterloo as a feature-rich and usable graph database management system (GDBMS). Kùzu is available under a permissible license. So try it out and help us make it better! We welcome your feedback and feature requests.

## Installation

| Language | Installation                                                           |
| -------- |------------------------------------------------------------------------|
| Python | `pip install kuzu`                                                     |
| NodeJS | `npm install kuzu`                                                     |
| Rust   | `cargo add kuzu`                                                       |
| Java   | [jar file](https://github.com/kuzudb/kuzu/releases/latest)             |
| C/C++  | [precompiled binaries](https://github.com/kuzudb/kuzu/releases/latest) |
| CLI    | [precompiled binaries](https://github.com/kuzudb/kuzu/releases/latest) |

To learn more about installation, see our [Installation](https://kuzudb.com/docusaurus/installation/) page.

## Getting Started

Refer to our [Getting Started](https://kuzudb.com/docusaurus/getting-started/) page for your first example.

More information can be found at
- [Data Import](https://kuzudb.com/docusaurus/data-import/)
- [Cypher Reference](https://kuzudb.com/docusaurus/cypher/)
- [Client APIs](https://kuzudb.com/docusaurus/client-apis/)

## Build from Source

Instructions can be found at [Build Kùzu from Source](https://kuzudb.com/docusaurus/development/building-kuzu).

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
You can contact us at [contact@kuzudb.com](mailto:contact@kuzudb.com) or [join our Slack workspace](https://join.slack.com/t/kuzudb/shared_invite/zt-1w0thj6s7-0bLaU8Sb~4fDMKJ~oejG_g).
