#!/bin/bash

# Collect source files
tar --exclude="$(pwd)" \
    --exclude="./bazel-*" \
    --exclude="./scripts" \
    --exclude="./.?*" \
    -cf\
    graphflowdb-source.tar\
    -C ../../. .
rm -rf graphflowdb-source && mkdir graphflowdb-source
tar -xf graphflowdb-source.tar -C ./graphflowdb-source
rm -rf graphflowdb-source.tar

# Add all files under current directory
touch sdist.tar
tar -cf sdist.tar --exclude=./sdist.tar .
rm -rf sdist && mkdir sdist
tar -xf sdist.tar -C ./sdist
rm -rf sdist.tar

# Create tar.gz for PyPI
rm -rf graphflowdb.tar.gz
tar -czf graphflowdb.tar.gz sdist
rm -rf sdist graphflowdb-source
