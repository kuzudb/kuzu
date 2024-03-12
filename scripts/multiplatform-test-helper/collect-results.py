#!/usr/bin/env python3

import os
import sys
import pandas as pd
import json


def main():
    if len(sys.argv) != 2:
        print("Usage: collect-results.py <results_dir>")
        sys.exit(1)
    if not os.path.isdir(sys.argv[1]):
        print(f"Error: {sys.argv[1]} is not a directory")
        sys.exit(1)
    results_dir = sys.argv[1]
    results_df_hash = {}
    results_exit_codes_hash = {}
    results_summary = {}
    stages = []
    for root, _, files in os.walk(results_dir):
        for csv_file in files:
            if not csv_file.endswith(".csv"):
                continue
            platform = csv_file.split(".")[0]
            df = pd.read_csv(os.path.join(root, csv_file), header=None)
            df.columns = ["stage", "exit_code"]
            results_df_hash[platform] = df

    for platform, df in results_df_hash.items():
        for stage, exit_code in df.values:
            if stage not in stages:
                stages.append(stage)
            if platform not in results_exit_codes_hash:
                results_exit_codes_hash[platform] = {}
            results_exit_codes_hash[platform][stage] = int(exit_code)

    for platform in results_df_hash.keys():
        results_summary[platform] = []
        for stage in stages:
            status = (
                "✅"
                if stage in results_exit_codes_hash[platform]
                and results_exit_codes_hash[platform][stage] == 0
                else "❌"
            )
            results_summary[platform].append({"stage": stage, "status": status})

    summary_df = {"stage": stages}
    for platform, summary in results_summary.items():
        df = pd.DataFrame(summary)
        status = df["status"]
        summary_df[platform] = status
    summary_df = pd.DataFrame(summary_df)
    summary_df.index = summary_df["stage"]
    del summary_df["stage"]
    summary_df.index.name = None

    markdown = summary_df.to_markdown()
    with open("results.md", "w") as f:
        f.write(markdown)

    with open("results.json", "w") as f:
        json.dump(results_summary, f, indent=4)


if __name__ == "__main__":
    main()
