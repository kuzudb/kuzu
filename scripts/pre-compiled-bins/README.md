# Intro
The script in this directory builds pre-compiled library and CLI and collects header for release. It generates the following files:
- `headers`: the headers for release in a directory;
- `kuzu.hpp`: the merged C++ header for release;
- `kuzu.h`: the C header for release;
- `libkuzu.so` / `libkuzu.dylib`: the shared library for both C and C++ API;
- `kuzu`: the database shell.
# Usage:
- Install dependency: `pip3 install networkx`.
- Run build: `python3 build.py`.
# Run on CI:
Currently we use the same environment to build the pre-compiled binaries as Python wheels. Please refer to the instructions in [this file](../pip-package/README.md).