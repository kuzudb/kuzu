#!/bin/bash

for num_workers in 1 4 8 16 32 48; do
    for batch_size in 16 32 64 128 256 512; do
        python3 benchmark_pyg.py ~/Developer/pyg_ogb_mag $num_workers $batch_size 10 100
    done
done
