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

### Building on Windows
Currently MSVC is the only supported compiler:

- In addition to the dependencies listed above, you will also need GNU Make and Ninja (E.g. with [Chocolatey](https://community.chocolatey.org/): `choco install make ninja`).
- Build from within a "Visual Studio Developer Command Prompt" (or manually run vcvars64.bat for cmd or Launch-VsDevShell.ps1 for powershell. See [here](https://learn.microsoft.com/en-us/cpp/build/how-to-enable-a-64-bit-visual-cpp-toolset-on-the-command-line?view=msvc-170) and [here](https://learn.microsoft.com/en-us/visualstudio/ide/reference/command-prompt-powershell?view=vs-2022) for details).
- Run `make release`, or the commands listed in the previous section.

You can also build within Visual Studio, as long as you run `make release` first (or `make debug`), and then use [the CMake plugin](https://learn.microsoft.com/en-us/cpp/build/cmake-projects-in-visual-studio).

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
