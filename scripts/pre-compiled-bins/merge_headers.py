import os
import logging
from pathlib import Path
import networkx as nx

logging.basicConfig(level=logging.DEBUG)

HEADER_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), 'headers')
)
OUTPUT_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), 'kuzu.hpp')
)

logging.debug('HEADER_PATH: %s', HEADER_PATH)
logging.debug('OUTPUT_PATH: %s', OUTPUT_PATH)


def build_dependency_graph():
    graph = nx.DiGraph()
    for header_path in Path(HEADER_PATH).rglob('*.h'):
        header_name = header_path.name
        with open(header_path, 'r') as f:
            for line in f.readlines():
                if not line.startswith('#include "'):
                    continue
                contained_header_name = Path(line.split('"')[1]).name
                graph.add_edge(header_name, contained_header_name)
    return graph


def generate_merged_headers():
    logging.info('Building dependency graph...')
    graph = build_dependency_graph()
    logging.info('Topological sorting...')
    topo_order = list(reversed(list(nx.topological_sort(graph))))
    logging.info('Writing merged headers...')
    with open(OUTPUT_PATH, 'w') as f:
        f.write('#pragma once\n')
        for header_name in topo_order:
            header_path = os.path.join(HEADER_PATH, header_name)
            with open(header_path, 'r') as f2:
                # Skip lines that start with #pragma once and #include
                for line in f2.readlines():
                    if line.startswith('#pragma once'):
                        continue
                    if line.startswith('#include "'):
                        continue
                    f.write(line)
            f.write('\n')
    logging.info('Done!')


if __name__ == '__main__':
    generate_merged_headers()
