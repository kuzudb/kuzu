#!/bin/bash

PLATFORM="manylinux2014_x86_64"

rm -rf wheelhouse kuzu.tar.gz && /opt/python/cp311-cp311/bin/python ./package_tar.py kuzu.tar.gz
mkdir wheelhouse

# Build wheels, excluding pypy platforms and python 3.6
for PYBIN in /opt/python/*/bin; do
    if [[ $PYBIN == *"pypy"* ]] || [[ $PYBIN == *"cp36"* ]]; then
        continue
    fi
    echo "Building wheel for $PYBIN..."
    "${PYBIN}/pip" wheel kuzu.tar.gz --no-deps -w wheelhouse/
done

for whl in wheelhouse/*.whl; do
    auditwheel repair $whl --plat "$PLATFORM" -w wheelhouse/
done
