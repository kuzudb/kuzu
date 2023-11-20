#!/bin/bash

rm -rf cpp/docs cpp/headers c/docs c/kuzu.h
python3 ../pre-compiled-bins/collect_files.py
python3 ../pre-compiled-bins/merge_headers.py
mv ../pre-compiled-bins/headers ./cpp/
cp ../../src/include/c_api/kuzu.h ./c/
cd cpp && doxygen Doxyfile
cd ..
cd c && doxygen Doxyfile
rm -rf cpp/headers c/kuzu.h
