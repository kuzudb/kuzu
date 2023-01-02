import sys
import argparse
import os
import sqlite3
import datetime
import logging
import subprocess

# dataset registration
datasets = {'ldbc-sf10', 'ldbc-sf100'}

datasets_path = {
    'ldbc-sf10-ku': '/home/x74feng/CI/ldbc-sf10',
    'ldbc-sf100-ku': '/home/x74feng/CI/ldbc-sf100'
}

serialized_graphs_path = {
    'ldbc-sf10-ku': '/home/x74feng/CI/ldbc-sf10-serialized',
    'ldbc-sf100-ku': '/home/x74feng/CI/ldbc-sf100-serialized'
}

benchmark_server_dir = '/home/x74feng/CI/server'
benchmark_log_dir = benchmark_server_dir + '/data/logs'
benchmark_files = os.getenv("GITHUB_WORKSPACE") + '/benchmark/queries'
kuzu_benchmark_tool = os.getenv("GITHUB_WORKSPACE") + '/build/release/tools/benchmark/kuzu_benchmark'

# benchmark configuration
num_warmup = 1
num_run = 5


class QueryBenchmark:
    def __init__(self, benchmark_log, group_name='NULL'):
        self.name = os.path.basename(benchmark_log).split('_')[0]
        self.group = group_name
        self.status = []
        self.compiling_time = []
        self.execution_time = []
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

    def insert_db(self, run_num):
        insert_query_record = '''INSERT INTO benchmark_result
                                 (query_name, status, compiling_time, execution_time, run_id, query_group, query_seq) 
                                 values(?, ?, ?, ?, ?, ?, ?);'''
        con = sqlite3.connect(benchmark_server_dir + '/benchmark.db')
        cur = con.cursor()
        for index, record in enumerate(self.status):
            if record == 'pass':
                cur.execute(insert_query_record,
                            (self.name, record, self.compiling_time[index],
                             self.execution_time[index], int(run_num), self.group, int(index + 1)))
            else:
                cur.execute(insert_query_record,
                            (self.name, record, 'NULL', 'NULL',
                             int(run_num), self.group, int(index + 1)))
        con.commit()
        con.close()


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
                elif line.startswith('expectedNumOutput'):  # parse number of output tuples
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


def get_run_num():
    if not os.path.exists(benchmark_server_dir + '/benchmark.db'):
        logging.error("Benchmark db not found! PATH: " + benchmark_server_dir + '/benchmark.db')
        sys.exit(1)
    try:
        query = 'SELECT MAX(run_id) FROM run_info'
        con = sqlite3.connect(benchmark_server_dir + '/benchmark.db')
        cur = con.cursor()
        result_tuple = cur.execute(query).fetchone()
        if result_tuple[0] is None:
            return 1
        else:
            return result_tuple[0] + 1

    except:
        return 1


def run_kuzu(serialized_graph_path):
    for group, _ in benchmark_group.group_to_benchmarks.items():
        benchmark_cmd = [
            kuzu_benchmark_tool,
            '--dataset=' + serialized_graph_path,
            '--benchmark=' + benchmark_files + '/' + group,
            '--warmup=' + str(num_warmup),
            '--run=' + str(num_run),
            '--out=' + benchmark_log_dir + '/' + group,
            '--bm-size=81920',
            '--profile'
        ]
        process = subprocess.Popen(tuple(benchmark_cmd), stdout=subprocess.PIPE)
        for line in iter(process.stdout.readline, b''):
            print(line.decode("utf-8"), end='')
        process.communicate()[0]
        if process.returncode != 0:
            print()
            logging.error("An error has occurred while running benchmark!")
            logging.error("Command: " + ' '.join(benchmark_cmd))
            sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default='ldbc-sf10', help='dataset to run benchmark')
    parser.add_argument('--thread', default='1', help='number of threads to run benchmark')
    parser.add_argument('--note', default='automated benchmark run', help='note about this run')
    return parser.parse_args()


def upload_run_info():
    insert_run_info_query = 'INSERT INTO run_info (commit_id, run_timestamp, note, dataset) values(?, ?, ?, ?)'
    con = sqlite3.connect(benchmark_server_dir + '/benchmark.db')
    cur = con.cursor()
    cur.execute(insert_run_info_query,
                (os.environ.get('GITHUB_SHA'), datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), args.note,
                 args.dataset))
    con.commit()
    con.close()


def upload_query_info(run_num):
    for path in os.scandir(benchmark_log_dir):
        if path.is_dir():
            for filename in os.listdir(path):
                if 'log' not in filename:
                    continue
                queryBenchmark = QueryBenchmark(os.path.join(path, filename), path.name)
                queryBenchmark.insert_db(run_num)


def upload_benchmark_result(run_num):
    upload_run_info()
    upload_query_info(run_num)


if __name__ == '__main__':
    if not os.path.exists(benchmark_server_dir):
        logging.error("Benchmark Server Dir not found! PATH: " + benchmark_server_dir)
        sys.exit(1)

    args = parse_args()
    run_num = get_run_num()
    benchmark_log_dir = benchmark_log_dir + "/run" + str(run_num)
    if not os.path.exists(benchmark_log_dir):
        os.mkdir(benchmark_log_dir)
    benchmark_files = benchmark_files + '/' + args.dataset
    dataset_path = datasets_path[args.dataset + '-ku']

    # load benchmark
    benchmark_group = BenchmarkGroup(benchmark_files)
    benchmark_group.load()

    run_kuzu(serialized_graphs_path[args.dataset + '-ku'])

    # upload benchmark result and logs
    upload_benchmark_result(run_num)
