# Click Benchmarks

[ClickBench datasets](https://github.com/ClickHouse/ClickBench/?tab=readme-ov-file#data-loading))

At the moment, the benchmarks are only working with the CSV dataset, however this must be loaded in single-threaded mode since there are quoted newlines, so it is very slow to load (using parquet would be faster, but the official parquet file stores timestamps in int64 columns which we don't convert implicitly.

The script benchmark.sh will run the entire benchmark pipeline, including downloading and importing the dataset.
