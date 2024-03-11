#!/usr/bin/env python3

import os
import sys
import pandas as pd

def main():
    if len(sys.argv) != 2:
        print("Usage: collect-results.py <results-dir>")
        sys.exit(1)

    results_dir = sys.argv[1]
    results = {}

    for root, dirs, files in os.walk(results_dir):
        for file in files:
            if file.endswith(".csv"):
                results[file] = pd.read_csv(os.path.join(root, file))
