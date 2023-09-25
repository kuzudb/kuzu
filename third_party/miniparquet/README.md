# miniparquet
[![Travis](https://api.travis-ci.org/hannesmuehleisen/miniparquet.svg?branch=master)](https://travis-ci.org/hannesmuehleisen/miniparquet)
[![CRAN
status](https://www.r-pkg.org/badges/version/miniparquet)](https://cran.r-project.org/package=miniparquet)
[![](http://cranlogs.r-pkg.org/badges/miniparquet)](https://dgrtwo.shinyapps.io/cranview/)

`miniparquet` is a reader for a common subset of Parquet files. miniparquet only supports rectangular-shaped data structures (no nested tables) and only the Snappy compression scheme. miniparquet has no (zero, none, 0) [external dependencies](https://research.swtch.com/deps) and is very lightweight. It compiles in seconds to a binary size of under 1 MB.

## Installation
Miniparquet comes as C++ library, a Python package and a R package. Install the R package like so:

`devtools::install_github("hannesmuehleisen/miniparquet")`

The C++ library can be built by typing `make`.

The Python package is installed using `python setup.py install`


## Usage
Use the R package like so: `df <- miniparquet::parquet_read("example.parquet")`

Folders of similar-structured Parquet files (e.g. produced by Spark) can be read like this:

`df <- data.table::rbindlist(lapply(Sys.glob("some-folder/part-*.parquet"), miniparquet::parquet_read))`

If you find a file that should be supported but isn't, please open an issue here with a link to the file.

Use the Python package like so: `miniparquet.read('example.parquet')`. You can convert the result to a Pandas dataframe like so: `pandas.DataFrame.from_dict(miniparquet.read('example.parquet'))`


## Performance
`miniparquet` is quite fast, on my laptop (I7-4578U) it can read compressed Parquet files at over 200 MB/s using only a single thread. Previously, there was a comparision with the arrow package here, but it appeared that results were caused by a bug which is fixed.