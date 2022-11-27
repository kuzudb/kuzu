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
For development, Kùzu requires Cmake(>=3.11), Python 3, and a compiler that supports `C++20`. 

### Build

- To do a full clean build
    - `make clean`
    - `make NUM_THREADS=x`
- To test (after running the build)
    - `make test`


## Code of Conduct
We are developing Kùzu in a publicly open github repo and encourage everyone 
to comment and discuss topics related to Kùzu. However we ask everyone to be
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
