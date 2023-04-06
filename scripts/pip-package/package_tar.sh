#!/bin/bash
# Remove existing tar.gz
rm -rf kuzu.tar.gz

# Add necessary files and directories
mkdir -p kuzu
cp ../../LICENSE ./LICENSE.txt
cp ./README.md ./README_PYTHON_BUILD.md
cp ../../README.md ./README.md

curr_dir=$(pwd)
# Collect source files
(cd ../..; git archive --format=tar -o $curr_dir/kuzu-source.tar HEAD)
rm -rf kuzu-source && mkdir kuzu-source
tar -xf kuzu-source.tar -C ./kuzu-source
rm -rf kuzu-source.tar

# Add all files under current directory
touch sdist.tar
tar -cf sdist.tar \
    --exclude=./sdist.tar \
    --exclude="./.?*" \
    --exclude="./Dockerfile" \
    --exclude="./README_PYTHON_BUILD.md" \
    --exclude="./*.sh" .
rm -rf sdist && mkdir sdist
tar -xf sdist.tar -C ./sdist
(cd ./sdist; python3 setup.py egg_info)
rm -rf sdist.tar

# Create tar.gz for PyPI
rm -rf kuzu.tar.gz
tar -czf kuzu.tar.gz sdist

# Clean up
rm -rf sdist kuzu-source
rm -rf LICENSE.txt
rm -rf kuzu
mv README_PYTHON_BUILD.md README.md
