#!/bin/bash

# Output temporary file
TMP_FILE="tmp_first_dataset_lines.txt"

# Clear the tmp file if it exists
> "$TMP_FILE"

# List of test files
TEST_FILES=(
/Users/tanvirgahunia/work/kuzu/test/test_files/agg/distinct_agg.test
/Users/tanvirgahunia/work/kuzu/test/test_files/agg/hash.test
/Users/tanvirgahunia/work/kuzu/test/test_files/agg/simple.test
/Users/tanvirgahunia/work/kuzu/test/test_files/comment/comment.test
/Users/tanvirgahunia/work/kuzu/test/test_files/copy/copy_node_csv.test
/Users/tanvirgahunia/work/kuzu/test/test_files/copy/copy_node_parquet.test
/Users/tanvirgahunia/work/kuzu/test/test_files/copy/copy_pk_long_string_parquet.test
/Users/tanvirgahunia/work/kuzu/test/test_files/copy/copy_to_csv.test
/Users/tanvirgahunia/work/kuzu/test/test_files/csv/compressed_csv.test
/Users/tanvirgahunia/work/kuzu/test/test_files/csv/dialect_detection.test
/Users/tanvirgahunia/work/kuzu/test/test_files/csv/multiline_quotes.test
/Users/tanvirgahunia/work/kuzu/test/test_files/ddl/ddl.test
/Users/tanvirgahunia/work/kuzu/test/test_files/dml_node/create/create_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/dml_rel/create/create_read_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/dml_rel/delete/delete_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/dml_rel/merge/merge_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/dml_rel/set/set_read_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/dml_rel/set/set_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/exceptions/copy/duplicated.test
/Users/tanvirgahunia/work/kuzu/test/test_files/exceptions/copy/ignore_invalid_row.test
/Users/tanvirgahunia/work/kuzu/test/test_files/exceptions/copy/null_pk.test
/Users/tanvirgahunia/work/kuzu/test/test_files/function/gds/rec_joins_small.test
/Users/tanvirgahunia/work/kuzu/test/test_files/function/list.test
/Users/tanvirgahunia/work/kuzu/test/test_files/function/offset.test
/Users/tanvirgahunia/work/kuzu/test/test_files/function/path.test
/Users/tanvirgahunia/work/kuzu/test/test_files/function/start_end_node.test
/Users/tanvirgahunia/work/kuzu/test/test_files/function/struct.test
/Users/tanvirgahunia/work/kuzu/test/test_files/generic_hash_join/basic.test
/Users/tanvirgahunia/work/kuzu/test/test_files/ldbc/basic.test
/Users/tanvirgahunia/work/kuzu/test/test_files/match/one_hop.test
/Users/tanvirgahunia/work/kuzu/test/test_files/match/undirected.test
/Users/tanvirgahunia/work/kuzu/test/test_files/nested_types/large_array.test
/Users/tanvirgahunia/work/kuzu/test/test_files/path/path.test
/Users/tanvirgahunia/work/kuzu/test/test_files/projection/single_label.test
/Users/tanvirgahunia/work/kuzu/test/test_files/recursive_join/multi_label.test
/Users/tanvirgahunia/work/kuzu/test/test_files/recursive_join/n_1.test
/Users/tanvirgahunia/work/kuzu/test/test_files/recursive_join/n_n.test
/Users/tanvirgahunia/work/kuzu/test/test_files/recursive_join/range_literal.test
/Users/tanvirgahunia/work/kuzu/test/test_files/transaction/create_node/create_tinysnb_checkpoint.test
/Users/tanvirgahunia/work/kuzu/test/test_files/transaction/create_rel/create_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/transaction/ddl/ddl_tinysnb.test
/Users/tanvirgahunia/work/kuzu/test/test_files/unwind/unwind.test
)

# Extract -DATASET line from each file
for file in "${TEST_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        dataset_line=$(grep -m 1 '^[-]DATASET' "$file")
        echo "$dataset_line" >> "$TMP_FILE"
    else
        echo "File not found: $file" >&2
    fi
done

echo "Extracted -DATASET lines saved to $TMP_FILE"

