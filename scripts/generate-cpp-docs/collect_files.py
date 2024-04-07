import os
import sys
import shutil
import logging
from hashlib import md5
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

HEADER_BASE_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../src/include'))
MAIN_HEADER_PATH = os.path.realpath(
    os.path.join(HEADER_BASE_PATH, 'main'))
START_POINT = os.path.realpath(
    os.path.join(MAIN_HEADER_PATH, 'kuzu.h')
)
JSON_HEADER_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../third_party/nlohmann_json/json_fwd.hpp'))
HEADER_TARGET_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), 'headers')
)

logging.debug('HEADER_BASE_PATH: %s', HEADER_BASE_PATH)
logging.debug('MAIN_HEADER_PATH: %s', MAIN_HEADER_PATH)
logging.debug('START_POINT: %s', START_POINT)
logging.debug('JSON_HEADER_PATH: %s', JSON_HEADER_PATH)


def collect_header_file_path_recurse(start_point):
    global processed_header_files
    global header_map
    if start_point in processed_header_files:
        return []
    curr_header_files = []
    with open(start_point, 'r') as f:
        for line in f.readlines():
            if not line.startswith('#include "'):
                continue
            header_path = os.path.normpath(line.split('"')[1])
            header_real_path = None
            # Special case for json_fwd.hpp
            if header_path == 'json_fwd.hpp':
                header_real_path = JSON_HEADER_PATH
                logging.debug('Found header: %s at %s',
                              header_path, header_real_path)
            else:
                # Check if the header is in the current directory
                start_point_dir = os.path.dirname(start_point)
                header_real_path = os.path.join(
                    start_point_dir, header_path)
                if os.path.exists(header_real_path):
                    logging.debug('Found header: %s at %s',
                                  header_path, header_real_path)
                else:
                    # Check if the header is in the include directory
                    header_real_path = os.path.join(
                        HEADER_BASE_PATH, header_path)
                    if os.path.exists(header_real_path):
                        logging.debug('Found header: %s at %s',
                                      header_path, header_real_path)
            if header_real_path is None:
                logging.error('Could not find header: %s', header_path)
                sys.exit(1)
            curr_header_files.append(header_real_path)
            if start_point not in header_map:
                header_map[start_point] = {}
            header_map[start_point][header_path] = header_real_path
    for header_file in curr_header_files:
        curr_header_files += collect_header_file_path_recurse(header_file)

    processed_header_files.add(start_point)
    return curr_header_files


def collect_header_file_paths():
    global processed_header_files
    global header_map
    processed_header_files = set()
    header_map = {}
    collect_header_file_path_recurse(START_POINT)


def copy_header(header_real_path):
    global copied_headers
    if header_real_path in copied_headers:
        return copied_headers[header_real_path]
    header_name = os.path.basename(header_real_path)
    # Rename the header if it is already copied
    if os.path.exists(os.path.join(HEADER_TARGET_PATH, header_name)):
        header_name = md5(header_real_path.encode()).hexdigest() + '.h'
    target_path = os.path.join(HEADER_TARGET_PATH, header_name)
    shutil.copyfile(header_real_path, target_path)
    copied_headers[header_real_path] = header_name
    logging.debug('Copied header: %s to %s', header_real_path, target_path)
    return header_name


def copy_headers():
    global header_map
    global copied_headers
    copied_headers = {}
    if os.path.exists(HEADER_TARGET_PATH):
        shutil.rmtree(HEADER_TARGET_PATH, ignore_errors=True)
    os.makedirs(HEADER_TARGET_PATH)
    for src_header in header_map:
        src_header_copied_name = copy_header(src_header)
        src_header_copied_path = os.path.join(
            HEADER_TARGET_PATH, src_header_copied_name)
        file = Path(src_header_copied_path)
        for original_header_path in header_map[src_header]:
            header_real_path = header_map[src_header][original_header_path]
            header_name = copy_header(header_real_path)
            file.write_text(file.read_text().replace(
                original_header_path, header_name))


if __name__ == '__main__':
    logging.info('Collecting header files...')
    collect_header_file_paths()
    logging.info('Copying header files...')
    copy_headers()
    logging.info('Done!')
