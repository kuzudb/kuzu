#!/usr/bin/bash

CD=`dirname "$0"`
DATASET_DIR=$CD/../dataset
python3 $CD/../benchmark/serializer.py DemoDB $DATASET_DIR/demo-db/parquet $DATASET_DIR/binary-demo --single-thread
python3 $CD/../benchmark/serializer.py TinySNB $DATASET_DIR/tinysnb $DATASET_DIR/databases/tinysnb --single-thread
