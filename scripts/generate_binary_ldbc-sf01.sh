#!/bin/bash

CD=`dirname "$0"`
DATASET_DIR=$CD/../dataset
python3 $CD/../benchmark/serializer.py LDBC-SF01 $DATASET_DIR/ldbc-sf01 $CD/../ldbc01
