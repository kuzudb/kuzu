#!/bin/bash

while read -r query; do
    sync
    sudo sysctl vm.drop_caches=3 > /dev/null

    ./query.py <<< "${query}"
done < queries.cypher
