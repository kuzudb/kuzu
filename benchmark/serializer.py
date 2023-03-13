import os
import logging
import shutil
import sys
import subprocess

base_dir = os.path.dirname(os.path.realpath(__file__))
kuzu_exec_path = os.path.join(
    base_dir, '..', 'build', 'release', 'tools', 'shell', 'kuzu_shell')


def _get_kuzu_version():
    cmake_file = os.path.join(base_dir, '..', 'CMakeLists.txt')
    with open(cmake_file) as f:
        for line in f:
            if line.startswith('project(Kuzu VERSION'):
                return line.split(' ')[2].strip()


def serialize(dataset_name, dataset_path, serialized_graph_path):
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

    for s in serialize_queries:
        logging.info('Executing query: %s', s)
        try:
            # Run kuzu shell one query at a time. This ensures a new process is
            # created for each query to avoid memory leaks.
            subprocess.run([kuzu_exec_path, serialized_graph_path],
                           input=(s + ";" + "\n").encode("ascii"), check=True)
        except subprocess.CalledProcessError as e:
            logging.error('Error executing query: %s', s)
            raise e

    with open(os.path.join(serialized_graph_path, 'version.txt'), 'w') as f:
        f.write(bin_version)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dataset_name = sys.argv[1]
    dataset_path = sys.argv[2]
    serialized_graph_path = sys.argv[3]
    try:
        serialize(dataset_name, dataset_path, serialized_graph_path)
    except Exception as e:
        logging.error('Error serializing dataset %s', dataset_name)
        sys.exit(1)
    finally:
        shutil.rmtree(os.path.join(base_dir, 'history.txt'),
                      ignore_errors=True)
