#!/bin/bash

CD=`dirname "$0"`
DATASET_DIR=$CD/../dataset
python3 $CD/../benchmark/serializer.py TinySNB $DATASET_DIR/tinysnb $CD/../tinysnb
