import shutil
import subprocess
import logging
import os
import argparse
import sys
import psutil
from serializer import _get_kuzu_version
import multiprocessing

# Get the number of CPUs, try to use sched_getaffinity if available to account 
# for Docker CPU limits
try:
    cpu_count = len(os.sched_getaffinity(0))
except AttributeError:
    cpu_count = multiprocessing.cpu_count()

# Use 90% of the available memory size as bm-size
# First try to read the memory limit from cgroup to account for Docker RAM 
# limit, if not available use the total memory size
try:
    # cgroup v2
    max_memory = int(open("/sys/fs/cgroup/memory.max").readline().strip())
except FileNotFoundError:
    try:
        # cgroup v1
        max_memory = int(
            open("/sys/fs/cgroup/memory/memory.limit_in_bytes").readline().strip())
    except FileNotFoundError:
        max_memory = psutil.virtual_memory().total

bm_size = int((max_memory / 1024 ** 2) * .9)
base_dir = os.path.dirname(os.path.realpath(__file__))

# import kuzu Python API
sys.path.append(os.path.join(base_dir, '..', '..'))
import tools.python_api.build.kuzu as kuzu

# dataset registration
datasets = {'0.1', '0.3', '1', '3', '10', '30', '100'}

csv_base_dir = os.path.join(os.getenv('CSV_DIR'), 'lsqb-datasets')
serialized_base_dir = os.getenv('SERIALIZED_DIR')
benchmark_files = os.path.join(base_dir, 'queries')

if csv_base_dir is None:
    logging.error("CSV_DIR is not set, exiting...")
    sys.exit(1)
if serialized_base_dir is None:
    logging.error("SERIALIZED_DIR is not set, exiting...")
    sys.exit(1)

datasets_path = {
    'lsqb-sf0.1-ku': os.path.join(csv_base_dir,  'social-network-sf0.1-projected-fk'),
    'lsqb-sf0.3-ku': os.path.join(csv_base_dir,  'social-network-sf0.3-projected-fk'),
    'lsqb-sf1-ku': os.path.join(csv_base_dir,  'social-network-sf1-projected-fk'),
    'lsqb-sf3-ku': os.path.join(csv_base_dir,  'social-network-sf3-projected-fk'),
    'lsqb-sf10-ku': os.path.join(csv_base_dir,  'social-network-sf10-projected-fk'),
    'lsqb-sf30-ku': os.path.join(csv_base_dir,  'social-network-sf30-projected-fk'),
    'lsqb-sf100-ku': os.path.join(csv_base_dir,  'social-network-sf100-projected-fk')
}

serialized_graphs_path = {
    'lsqb-sf0.1-ku': os.path.join(serialized_base_dir, 'lsqb-sf0.1-serialized'),
    'lsqb-sf0.3-ku': os.path.join(serialized_base_dir, 'lsqb-sf0.3-serialized'),
    'lsqb-sf1-ku': os.path.join(serialized_base_dir, 'lsqb-sf1-serialized'),
    'lsqb-sf3-ku': os.path.join(serialized_base_dir, 'lsqb-sf3-serialized'),
    'lsqb-sf10-ku': os.path.join(serialized_base_dir, 'lsqb-sf10-serialized'),
    'lsqb-sf30-ku': os.path.join(serialized_base_dir, 'lsqb-sf30-serialized'),
    'lsqb-sf100-ku': os.path.join(serialized_base_dir, 'lsqb-sf100-serialized'),
}

benchmark_result_dir = os.path.join("/tmp", 'benchmark_result')
shutil.rmtree(benchmark_result_dir, ignore_errors=True)
os.mkdir(benchmark_result_dir)


def serialize_dataset(dataset_name):
    dataset_path = datasets_path[dataset_name]
    serialized_graph_path = serialized_graphs_path[dataset_name]
    serializer_script = os.path.join(base_dir, "serializer.py")
    try:
        subprocess.run([sys.executable, serializer_script, dataset_name,
                       dataset_path, serialized_graph_path], check=True)
    except subprocess.CalledProcessError as e:
        logging.error("Failed to serialize dataset: %s", e)
        sys.exit(1)


def run_query(conn, query_spec):
    # Monitor initial system usage
    initial_ram_usage = psutil.virtual_memory().used

    try:
        results = conn.execute(query_spec)
        if results.has_next():
            result = results.get_next()
            compiling_time = results.get_compiling_time()
            execution_time = results.get_execution_time()
    except RuntimeError:
        raise TimeoutError

    # Monitor final system usage
    final_ram_usage = psutil.virtual_memory().used
    ram_change = max(0, final_ram_usage - initial_ram_usage)

    return execution_time, compiling_time, ram_change, result


def run_kuzu(sf, serialized_graph_path):
    numThreads = int(args.thread)

    db = kuzu.Database(serialized_graph_path)
    conn = kuzu.Connection(db, num_threads=numThreads)
    conn.set_query_timeout(600000) # Set timeout to 10 minutes

    with open(os.path.join(benchmark_result_dir, "results.csv"), "a+") as results_file:
        for i in range(1, 10):
            logging.info(f"Running query {i}")
            with open(os.path.join(base_dir, f"queries/q{i}.cypher"), "r") as query_file:
                query_spec = query_file.read()

                try:
                    # Warm up run
                    execution_time, compiling_time, memory, result = run_query(conn, query_spec)
                except TimeoutError:
                    logging.info("Query timed out after 10 minutes\n")
                    results_file.write(f"KuzuDB\t{i}\t{numThreads} threads\t{sf}\tTimeout\tNA\tNA\t\n")
                    results_file.flush()
                    continue

                execution_times = [execution_time]
                compiling_times = [compiling_time]
                memories = [memory]

                num_trials = 3

                # Run the queries multiple times
                for _ in range(num_trials):
                    execution_time, compiling_time, memory, result = run_query(conn, query_spec)
                    execution_times.append(execution_time)
                    compiling_times.append(compiling_time)
                    memories.append(memory)

                # Get avg execution time
                max_execution_time = max(execution_times)
                execution_times.remove(max_execution_time) # Remove the worst record
                execution_time = sum(execution_times) / len(execution_times)

                # Get avg compiling time
                max_compiling_time = max(compiling_times)
                compiling_times.remove(max_compiling_time)
                compiling_time = sum(compiling_times) / len(compiling_times)

                # Get avg memory
                filtered_memories = [x for x in memories if x != 0]
                if len(filtered_memories) == 0:
                    memory = 0
                else:
                    memory = sum(filtered_memories) / len(filtered_memories)

                # Log the result
                logging.info(f"Query execution successful. Output: {result[0]}")
                logging.info(f"Execution time (s): {execution_time / 1000:.4f}")
                logging.info(f"Compiling time (s): {compiling_time / 1000:.4f}")
                logging.info(f"Memory used: {memory / (1024 ** 3):.2f} GB\n")

                # Upload the result
                results_file.write(f"KuzuDB\t{i}\t{numThreads} threads\t{sf}\t{execution_time / 1000:.4f}\t{memory / (1024 ** 3):.2f} GB\t{result[0]}\n")
                results_file.flush()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sf', default='100',
                        help='scale factor to run benchmark')
    parser.add_argument('--thread', default=str(cpu_count),
                        help='number of threads to run benchmark')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    dataset_name = 'lsqb-sf' + args.sf + '-ku'
    dataset_path = datasets_path[dataset_name]

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Running benchmark for scale factor %s", args.sf)
    logging.info("Database version: %s", _get_kuzu_version())
    logging.info("CPU cores: %d", cpu_count)
    logging.info("Using %s threads", args.thread)
    logging.info("Total memory: %d GiB", max_memory / 1024 ** 3)
    logging.info("bm-size: %d MiB", bm_size)

    # serialize dataset
    serialize_dataset(dataset_name)

    logging.info("Running benchmark...")
    run_kuzu(args.sf, serialized_graphs_path[dataset_name])
    logging.info("Benchmark finished")
