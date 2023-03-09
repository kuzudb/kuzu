from pyarrow import csv
import pyarrow.parquet as pq

csv_files = ['dummy.csv']
read_options = csv.ReadOptions(autogenerate_column_names=True)
for csv_file in csv_files:
    table = csv.read_csv(csv_file, read_options=read_options)
    pq.write_table(table, csv_file.replace('.csv', '.parquet'))