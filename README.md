# kuzu

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
  
## CLI

- To start CLI `bazel run //tools/shell:kuzu -- -i <serialized-graph-path>`
- CLI built in commands
  :help get command list
  :clear clear shell
  :quit exit from shell
  :thread [thread]     number of threads for execution
  :bm_debug_info debug information about the buffer manager
  :system_debug_info debug information about the system

## Benchmark runner

Benchmark runner is designed to be used in kuzu-benchmark. Deirectly using benchmark runner is not recommended.

- benchmark file should have the following format

```
name example

query
MATCH (:Person)
RETURN COUNT(*)

expectedNumOutput 8

```

- To run benchmark
  runner `<benchmark-tool-binary-path> --dataset=<serialized-graph-path> --benchmark=<benchmark-file-or-folder>`
    - `--warmup=1` config number of warmup
    - `--run=5` config number of run
    - `--thread=1` config number of thread for execution

## Python Driver

Current python driver is under development. See `tools/python_api/test/example.py` for example usage. To run this
example `bazel run //tools/python_api:example`.

## Design Docs

[Design documents](https://drive.google.com/drive/folders/1Z5tYGGq9ivWWDyl8s5dbUs9eKNgi1baS?usp=sharing) for some of our
features and components.
