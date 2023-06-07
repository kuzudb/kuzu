from pyarrow import csv
import pyarrow.parquet as pq

csv_files = ['dummy.csv']
has_header = True
#  CSV:
#  has header? autogenerate_column_names=False
#  no header? autogenerate_column_names=True
read_options = csv.ReadOptions(autogenerate_column_names=not has_header)
parse_options = csv.ParseOptions(delimiter=",")
for csv_file in csv_files:
    table = csv.read_csv(csv_file, read_options=read_options,
                         parse_options=parse_options)
    pq.write_table(table, csv_file.replace('.csv', '.parquet'))
