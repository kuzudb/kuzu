# graphflowDB

## Compilation

### Bazel configuration

- Create bazel configuration file with `touch .bazelrc`
- Add bazel configuration `echo build --cxxopt="-std=c++2a" --cxxopt='-fopenmp' --linkopt='-lgomp' > .bazelrc`

### Bazel build

- To do a full clean build
    - `bazel clean`
    - `bazel build //...:all`
- To test
    - `bazel test //...:all`

## Dataset loading

- Dataset folder should contain a metadata.json with the following format

```
{
    "tokenSeparator": ",",
    "nodeFileDescriptions": [
        {
            "filename": "vPerson.csv",
            "label": "person",
            "primaryKey": "id"
        }
    ],
    "relFileDescriptions": [
        {
            "filename": "eKnows.csv",
            "label": "knows",
            "multiplicity": "MANY_MANY",
            "srcNodeLabels": [
                "person"
            ],
            "dstNodeLabels": [
                "person"
            ]
        },
    ]
}
```

- To serialize
  dataset `bazel runWorkerThread //src/loader:loader_runner <input-csv-path> <output-serialized-graph-path>`

## CLI

- To start CLI `bazel runWorkerThread //tools/shell:graphflowdb <serialized-graph-path>`
- CLI built in commands
    - `:help` get built in command lists
    - `:clear` clear screen
    - `:quit` exit from shell
    - `:thread` set number of threads used for execution

## Benchmark runner

Benchmark runner is designed to be used in graphflowdb-benchmark. Deirectly using benchmark runner is not recommended.

- benchmark file should have the following format

```
name example

query
MATCH (:Person)
RETURN COUNT(*)

expectedNumOutput 8

```

- To runWorkerThread benchmark
  runner `<benchmark-tool-binary-path> --dataset=<serialized-graph-path> --benchmark=<benchmark-file-or-folder>`
    - `--warmup=1` config number of warmup
    - `--runWorkerThread=5` config number of runWorkerThread
    - `--thread=1` config number of thread for execution
