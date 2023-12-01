import os
import logging
import shutil
import sys
import subprocess
import re

base_dir = os.path.dirname(os.path.realpath(__file__))
kuzu_exec_path = os.path.join(
    base_dir, '..', 'build', 'release', 'tools', 'shell', 'kuzu_shell')


def _get_kuzu_version():
    cmake_file = os.path.join(base_dir, '..', 'CMakeLists.txt')
    with open(cmake_file) as f:
        for line in f:
            if line.startswith('project(Kuzu VERSION'):
                return line.split(' ')[2].strip()


def serialize(dataset_name, dataset_path, serialized_graph_path, benchmark_copy_log_dir):
    bin_version = _get_kuzu_version()

    if not os.path.exists(serialized_graph_path):
        os.mkdir(serialized_graph_path)

    if os.path.exists(os.path.join(serialized_graph_path, 'version.txt')):
        with open(os.path.join(serialized_graph_path, 'version.txt')) as f:
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

    with open(os.path.join(base_dir, 'serialize.cypher'), 'r') as f:
        serialize_queries = f.readlines()
    serialize_queries = [q.strip().replace('{}', dataset_path)
                         for q in serialize_queries]

    table_types = {}

    for s in serialize_queries:
        logging.info('Executing query: %s', s)
        create_match = re.match('create\s+(.+?)\s+table\s+(.+?)\s*\(', s, re.IGNORECASE)
        # Run kuzu shell one query at a time. This ensures a new process is
        # created for each query to avoid memory leaks.
        process = subprocess.Popen([kuzu_exec_path, serialized_graph_path],
            stdin=subprocess.PIPE, stdout=sys.stdout if create_match else subprocess.PIPE)
        process.stdin.write((s + ";" + "\n").encode("ascii"))
        process.stdin.close()
        if create_match:
            table_types[create_match.group(2)] = create_match.group(1).lower()
        else:
            copy_match = re.match('copy\s+(.+?)\s+from', s, re.IGNORECASE)
            filename = table_types[copy_match.group(1)] + '-' + copy_match.group(1).replace('_', '-') + '_log.txt'
            with open(os.path.join(benchmark_copy_log_dir, filename), 'ab') as f:
                for line in iter(process.stdout.readline, b''):
                    print(line.decode("utf-8"), end='', flush=True)
                    f.write(line)
        process.wait()
        if process.returncode != 0:
            logging.error('Error executing query: %s', s)
            raise subprocess.CalledProcessError

    with open(os.path.join(serialized_graph_path, 'version.txt'), 'w') as f:
        f.write(bin_version)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dataset_name = sys.argv[1]
    dataset_path = sys.argv[2]
    serialized_graph_path = sys.argv[3]
    benchmark_copy_log_dir = sys.argv[4]
    try:
        serialize(dataset_name, dataset_path, serialized_graph_path, benchmark_copy_log_dir)
    except Exception as e:
        logging.error('Error serializing dataset %s', dataset_name)
        logging.error(e)
        sys.exit(1)
    finally:
        shutil.rmtree(os.path.join(base_dir, 'history.txt'),
                      ignore_errors=True)
