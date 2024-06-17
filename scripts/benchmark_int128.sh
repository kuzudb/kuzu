set -u

benchmark_db() {
    NUM_NODES=$1
    echo "NUM_NODES="${NUM_NODES}
    rm -rf ./workspace
    python scripts/generate-int128_dataset.py ${NUM_NODES} $2
    ./build/release/tools/shell/kuzu ./workspace < dataset/int128-db/copy.cypher
    ls -l ./workspace
    echo $'DONE\n'
}

run_test() {
    for num_nodes in 10000000 100000000
    do
        benchmark_db ${num_nodes} $1
    done
}

run_test high > "$1-favourable.log"
run_test low > "$1-unfavourable.log"
