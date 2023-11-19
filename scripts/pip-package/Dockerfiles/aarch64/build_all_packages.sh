#!/bin/bash

PLATFORM="manylinux_2_28_aarch64"

rm -rf wheelhouse kuzu.tar.gz && /opt/python/cp311-cp311/bin/python ./package_tar.py kuzu.tar.gz
mkdir wheelhouse

for PYBIN in /opt/python/*/bin; do
    # The following lines only build cp311 wheels, for testing purpose only
    # if [[ $PYBIN != *"cp311"* ]]; then
    #     continue
    # fi

    # Build wheels, excluding pypy platforms and python 3.6
    if [[ $PYBIN == *"pypy"* ]] || [[ $PYBIN == *"cp36"* ]]; then
        continue
    fi
    echo "Building wheel for $PYBIN..."
    "${PYBIN}/pip" wheel kuzu.tar.gz --no-deps -w wheelhouse/
done

for whl in wheelhouse/*.whl; do
    auditwheel repair $whl --plat "$PLATFORM" -w wheelhouse/
done
