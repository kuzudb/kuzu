#!/bin/bash

# Collect source files
cp ../../LICENSE ./LICENSE.txt
tar --exclude="$(pwd)" \
    --exclude="./bazel-*" \
    --exclude="./scripts" \
    --exclude="./.?*" \
    -cf\
    kuzu-source.tar\
    -C ../../. .
rm -rf kuzu-source && mkdir kuzu-source
tar -xf kuzu-source.tar -C ./kuzu-source
rm -rf kuzu-source.tar

# Add all files under current directory
touch sdist.tar
tar -cf sdist.tar --exclude=./sdist.tar .
rm -rf sdist && mkdir sdist
tar -xf sdist.tar -C ./sdist
rm -rf sdist.tar

# Create tar.gz for PyPI
rm -rf kuzu.tar.gz
tar -czf kuzu.tar.gz sdist
rm -rf sdist kuzu-source
rm -rf LICENSE.txt
