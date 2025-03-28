#!/bin/bash

# Install

python -m venv venv
source venv/bin/activate || exit
pip install kuzu

# Load the data

wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

python load.py

# Run the queries

./run.sh 2>&1 | tee log.txt

ls -alh ./mydb

grep -P '^\d|Killed|Segmentation' log.txt | sed -r -e 's/^.*(Killed|Segmentation).*$/null\nnull\nnull/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }' > benchmarks.json
