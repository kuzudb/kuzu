import os
import logging
import shutil
import sys
import subprocess
import re
import argparse

base_dir = os.path.dirname(os.path.realpath(__file__))


def _get_kuzu_version():
    cmake_file = os.path.join(base_dir, '..', 'CMakeLists.txt')
    with open(cmake_file) as f:
        for line in f:
            if line.startswith('project(Kuzu VERSION'):
                return line.split(' ')[2].strip()


def serialize(kuzu_exec_path, dataset_name, dataset_path, serialized_graph_path, benchmark_copy_log_dir, single_thread: bool = False):
    bin_version = _get_kuzu_version()

    if not os.path.exists(serialized_graph_path):
        os.mkdir(serialized_graph_path)

    if os.path.exists(os.path.join(serialized_graph_path, 'version.txt')):
        with open(os.path.join(serialized_graph_path, 'version.txt'), encoding="utf-8") as f:
            dataset_version = f.readline().strip()
            if dataset_version == bin_version:
                logging.info(
                    'Dataset %s has version of %s, which matches the database version, skip serializing', dataset_name,
                    bin_version)
                return
            else:
                logging.info(
                    'Dataset %s has version of %s, which does not match the database version %s, serializing dataset...',
                    dataset_name, dataset_version, bin_version)
    else:
        logging.info('Dataset %s does not exist or does not have a version file, serializing dataset...', dataset_name)

    shutil.rmtree(serialized_graph_path, ignore_errors=True)
    os.mkdir(serialized_graph_path)

    serialize_queries = []
    if single_thread:
        serialize_queries.append("CALL THREADS=1")
    if os.path.exists(os.path.join(dataset_path, 'schema.cypher')):
        with open(os.path.join(dataset_path, 'schema.cypher'), 'r') as f:
            serialize_queries += f.readlines()
        with open(os.path.join(dataset_path, 'copy.cypher'), 'r') as f:
            serialize_queries += f.readlines()
    else:
        with open(os.path.join(base_dir, 'serialize.cypher'), 'r') as f:
            serialize_queries += f.readlines()
    serialize_queries = [q.strip().replace('{}', dataset_path)
                         for q in serialize_queries]
    serialize_queries = [q for q in serialize_queries if q]

    table_types = {}

    for s in serialize_queries:
        logging.info('Executing query: %s', s)
        create_match = re.match(r'create\s+(.+?)\s+table\s+(.+?)\s*\(', s, re.IGNORECASE)
        copy_match = re.match(r'copy\s+(.+?)\s+from', s, re.IGNORECASE)
        # Run kuzu shell one query at a time. This ensures a new process is
        # created for each query to avoid memory leaks.
        stdout = sys.stdout if create_match or not benchmark_copy_log_dir else subprocess.PIPE
        process = subprocess.Popen([kuzu_exec_path, serialized_graph_path],
            stdin=subprocess.PIPE, stdout=stdout, encoding="utf-8")
        process.stdin.write(s)
        process.stdin.close()
        if create_match:
            table_types[create_match.group(2)] = create_match.group(1).lower()
        elif copy_match:
            filename = table_types[copy_match.group(1)] + '-' + copy_match.group(1).replace('_', '-') + '_log.txt'
            if benchmark_copy_log_dir:
                os.makedirs(benchmark_copy_log_dir, exist_ok=True)
                with open(os.path.join(benchmark_copy_log_dir, filename), 'a', encoding="utf-8") as f:
                    for line in process.stdout.readlines():
                        print(line, end="", flush=True)
                        print(line, file=f)
        elif s == "CALL THREADS=1":
            pass
        else:
            raise RuntimeError(f"Unrecognized query {s}")

        process.wait()
        if process.returncode != 0:
            raise RuntimeError(f'Error {process.returncode} executing query: {s}')

    with open(os.path.join(serialized_graph_path, 'version.txt'), 'w', encoding="utf-8") as f:
        print(bin_version, file=f)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Serializes dataset to a kuzu database')
    parser.add_argument("dataset_name", help="Name of the dataset for dispay purposes")
    parser.add_argument("dataset_path", help="Input path of the dataset to serialize")
    parser.add_argument("serialized_graph_path", help="Output path of the database. Will be created if it does not exist already")
    parser.add_argument("benchmark_copy_log_dir", help="Optional directory to store copy logs", nargs="?")
    parser.add_argument("--single-thread", help="If true, copy single threaded, which makes the results more reproduceable", action="store_true")
    if sys.platform == "win32":
        default_kuzu_exec_path = os.path.join(
            base_dir, '..', 'build', 'release', 'tools', 'shell', 'kuzu_shell')
    else:
        default_kuzu_exec_path = os.path.join(
            base_dir, '..', 'build', 'release', 'tools', 'shell', 'kuzu')
    parser.add_argument("--kuzu-shell", help="Path of the kuzu shell executable. Defaults to the path as built in the default release build directory", default=default_kuzu_exec_path)
    args = parser.parse_args()
    args = parser.parse_args()

    try:
        serialize(args.kuzu_shell, args.dataset_name, args.dataset_path, args.serialized_graph_path, args.benchmark_copy_log_dir, args.single_thread)
    except Exception as e:
        logging.error(f'Error serializing dataset {args.dataset_name}')
        raise e
    finally:
        shutil.rmtree(os.path.join(base_dir, 'history.txt'),
                      ignore_errors=True)
