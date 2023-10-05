import requests
import shutil
import subprocess
import logging
import datetime
import os
import argparse
import sys
import psutil
from serializer import _get_kuzu_version
import multiprocessing
import re

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

# dataset registration
datasets = {'ldbc-sf10', 'ldbc-sf100'}

csv_base_dir = os.getenv('CSV_DIR')
serialized_base_dir = os.getenv('SERIALIZED_DIR')
is_dry_run = os.getenv('DRY_RUN') == 'true'
benchmark_files = os.path.join(base_dir, 'queries')

kuzu_benchmark_tool = os.path.join(
    base_dir, '..', 'build', 'release', 'tools', 'benchmark', 'kuzu_benchmark')

if csv_base_dir is None:
    logging.error("CSV_DIR is not set, exiting...")
    sys.exit(1)
if serialized_base_dir is None:
    logging.error("SERIALIZED_DIR is not set, exiting...")
    sys.exit(1)

benchmark_server_url = os.getenv('BENCHMARK_SERVER_URL')
if benchmark_server_url is None and not is_dry_run:
    logging.error("BENCHMARK_SERVER_URL is not set, exiting...")
    sys.exit(1)

jwt_token = os.getenv('JWT_TOKEN')
if jwt_token is None and not is_dry_run:
    logging.error("JWT_TOKEN is not set, exiting...")
    sys.exit(1)

datasets_path = {
    'ldbc-sf10-ku': os.path.join(csv_base_dir, 'ldbc-10', 'csv'),
    'ldbc-sf100-ku': os.path.join(csv_base_dir, 'ldbc-100', 'csv')
}

serialized_graphs_path = {
    'ldbc-sf10-ku': os.path.join(serialized_base_dir, 'ldbc-sf10-serialized'),
    'ldbc-sf100-ku': os.path.join(serialized_base_dir, 'ldbc-sf100-serialized')
}

benchmark_copy_log_dir = os.path.join("/tmp", 'benchmark_copy_logs')
shutil.rmtree(benchmark_copy_log_dir, ignore_errors=True)
os.mkdir(benchmark_copy_log_dir)

benchmark_log_dir = os.path.join("/tmp", 'benchmark_logs')
shutil.rmtree(benchmark_log_dir, ignore_errors=True)
os.mkdir(benchmark_log_dir)


# benchmark configuration
num_warmup = 1
num_run = 5


class CopyQueryBenchmark:
    def __init__(self, benchmark_copy_log):
        self.name = os.path.basename(benchmark_copy_log).split('_')[0]
        self.group = 'copy'
        self.status = 'pass'

        with open(benchmark_copy_log) as log_file:
            self.log = log_file.read()
        match = re.search('Time: (.*)ms \(compiling\), (.*)ms \(executing\)', self.log)
        self.compiling_time = float(match.group(1))
        self.execution_time = float(match.group(2))

    def to_json_dict(self):
        result = {
            'query_name': self.name,
            'query_group': self.group,
            'log': self.log,
            'records': [
                {
                    'status': self.status,
                    'compiling_time': self.compiling_time,
                    'execution_time': self.execution_time,
                    'query_seq': 3  # value > 2 required by server
                }
            ]
        }
        return result


class QueryBenchmark:
    def __init__(self, benchmark_log, group_name='NULL'):
        self.name = os.path.basename(benchmark_log).split('_')[0]
        self.group = group_name
        self.status = []
        self.compiling_time = []
        self.execution_time = []

        profile_log_path = os.path.join(os.path.dirname(
            benchmark_log), self.name + '_profile.txt')
        if os.path.exists(profile_log_path):
            with open(profile_log_path) as profile_file:
                self.profile = profile_file.read()
        else:
            self.profile = None
        with open(benchmark_log) as log_file:
            self.log = log_file.read()
        with open(benchmark_log) as log_file:
            for line in log_file:
                if ':' not in line:
                    continue
                key = line.split(':')[0]
                value = line.split(':')[1][1:-1]
                if key == 'Status':
                    self.status.append(value)
                elif key == 'Compiling time':
                    self.compiling_time.append(float(value))
                elif key == 'Execution time':
                    self.execution_time.append(float(value))

    def to_json_dict(self):
        result = {
            'query_name': self.name,
            'query_group': self.group,
            'log': self.log,
            'profile': self.profile,
            'records': []
        }

        for index, record in enumerate(self.status):
            curr_dict = {
                'status': record,
                'compiling_time': self.compiling_time[index] if record == 'pass' else None,
                'execution_time': self.execution_time[index] if record == 'pass' else None,
                'query_seq': int(index + 1)
            }
            result['records'].append(curr_dict)
        return result


class Benchmark:
    def __init__(self, path) -> None:
        self.query = ""
        self._load(path)

    def _load(self, path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] == '#':  # skip empty line or comment line
                    continue
                if line.startswith('name'):  # parse name
                    self.name = line.split(' ')[1]
                elif line.startswith('query'):  # parse query
                    line = next(f)
                    line = line.strip()
                    # query can be written in multiple lines
                    while line:
                        self.query += line + " "
                        line = next(f)
                        line = line.strip()
                # parse number of output tuples
                elif line.startswith('expectedNumOutput'):
                    self.expectedNumOutput = line.split(' ')[1]


class BenchmarkGroup:
    def __init__(self, base_dir) -> None:
        self.base_dir = base_dir
        self.group_to_benchmarks = {}

    def load(self):
        for group in os.listdir(self.base_dir):
            path = self.base_dir + '/' + group
            if os.path.isdir(path):
                benchmarks = self._load_group(path)
                self.group_to_benchmarks[group] = benchmarks
                if not os.path.exists(benchmark_log_dir + '/' + group):
                    os.mkdir(benchmark_log_dir + '/' + group)

    def _load_group(self, group_path):
        benchmarks = []
        for f in os.listdir(group_path):
            if f.endswith('.benchmark'):
                benchmarks.append(Benchmark(group_path + '/' + f))
        return benchmarks


def serialize_dataset(dataset_name):
    dataset_path = datasets_path[dataset_name]
    serialized_graph_path = serialized_graphs_path[dataset_name]
    serializer_script = os.path.join(base_dir, "serializer.py")
    try:
        subprocess.run([sys.executable, serializer_script, dataset_name,
                       dataset_path, serialized_graph_path, benchmark_copy_log_dir], check=True)
    except subprocess.CalledProcessError as e:
        logging.error("Failed to serialize dataset: %s", e)
        sys.exit(1)


def run_kuzu(serialized_graph_path):
    for group, _ in benchmark_group.group_to_benchmarks.items():
        benchmark_cmd = [
            kuzu_benchmark_tool,
            '--dataset=' + serialized_graph_path,
            '--benchmark=' + benchmark_files + '/' + group,
            '--warmup=' + str(num_warmup),
            '--run=' + str(num_run),
            '--out=' + benchmark_log_dir + '/' + group,
            '--bm-size=' + str(bm_size),
            '--thread=' + args.thread,
            '--profile'
        ]
        process = subprocess.Popen(
            tuple(benchmark_cmd), stdout=subprocess.PIPE)
        for line in iter(process.stdout.readline, b''):
            print(line.decode("utf-8"), end='', flush=True)
        process.wait()
        if process.returncode != 0:
            print()
            logging.error("An error has occurred while running benchmark!")
            logging.error("Command: " + ' '.join(benchmark_cmd))
            sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default='ldbc-sf100',
                        help='dataset to run benchmark')
    parser.add_argument('--thread', default=str(cpu_count),
                        help='number of threads to run benchmark')
    parser.add_argument(
        '--note', default='automated benchmark run', help='note about this run')
    return parser.parse_args()


def _get_git_revision_hash():
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode("utf-8").strip()
    except:
        return None


def get_run_info():
    return {
        'commit_id': os.environ.get('GITHUB_SHA', _get_git_revision_hash()),
        'run_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'note': args.note,
        'dataset': args.dataset
    }


def get_query_info():
    results = []
    for filename in os.listdir(benchmark_copy_log_dir):
        copy_query_benchmark = CopyQueryBenchmark(os.path.join(benchmark_copy_log_dir, filename))
        results.append(copy_query_benchmark.to_json_dict())
    for path in os.scandir(benchmark_log_dir):
        if path.is_dir():
            for filename in os.listdir(path):
                if 'log' not in filename:
                    continue
                query_benchmark = QueryBenchmark(
                    os.path.join(path, filename), path.name)
                results.append(query_benchmark.to_json_dict())
    return results


def upload_benchmark_result():
    run = get_run_info()
    queries = get_query_info()
    run['queries'] = queries

    response = requests.post(
        benchmark_server_url, json=run, headers={
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer ' + jwt_token
        }
    )
    if response.status_code != 200:
        logging.error(
            "An error has occurred while uploading benchmark result!")
        sys.exit(1)


if __name__ == '__main__':
    args = parse_args()
    benchmark_log_dir = benchmark_log_dir
    benchmark_files = benchmark_files + '/' + args.dataset
    dataset_path = datasets_path[args.dataset + '-ku']

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Running benchmark for dataset %s", args.dataset)
    logging.info("Database version: %s", _get_kuzu_version())
    logging.info("CPU cores: %d", cpu_count)
    logging.info("Using %s threads", args.thread)
    logging.info("Total memory: %d GiB", max_memory / 1024 ** 3)
    logging.info("bm-size: %d MiB", bm_size)

    # serialize dataset
    serialize_dataset(args.dataset + '-ku')

    # load benchmark
    benchmark_group = BenchmarkGroup(benchmark_files)
    benchmark_group.load()

    logging.info("Running benchmark...")
    run_kuzu(serialized_graphs_path[args.dataset + '-ku'])
    logging.info("Benchmark finished")

    if is_dry_run:
        logging.info("Dry run, skipping upload")
        sys.exit(0)

    # upload benchmark result and logs
    logging.info("Uploading benchmark result...")
    upload_benchmark_result()
    logging.info("Benchmark result uploaded")
