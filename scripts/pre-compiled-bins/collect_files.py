import os
import sys
import glob
import shutil
import pathlib
import logging
import stat
from hashlib import md5
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

MAIN_HEADER_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../src/'))
MAIN_DIR_NAME = os.path.basename(os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../')))

BAZEL_WORKSPACE_NAME = 'bazel-%s' % MAIN_DIR_NAME
EXTERNAL_HEADER_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../%s/external' % BAZEL_WORKSPACE_NAME))

MAIN_OBJ_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../bazel-bin/src'))
EXTERNAL_OBJ_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../bazel-bin/external'))

MAIN_EXE_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../../bazel-bin/tools/shell/kuzu'))

UNUSED_HEADER_EXCLUDE_PREFIXES = [
    'src/main',
]
DEPLOYMENT_HEADER_PREFIX = ''

if not os.path.exists(EXTERNAL_HEADER_PATH):
    logging.error('External header path does not exist: ' +
                  EXTERNAL_HEADER_PATH)
    sys.exit(1)
if not os.path.exists(MAIN_OBJ_PATH):
    logging.error('Main object path does not exist: ' + MAIN_OBJ_PATH)
    sys.exit(1)

if not os.path.exists(EXTERNAL_OBJ_PATH):
    logging.error('External object path does not exist: ' + EXTERNAL_OBJ_PATH)
    sys.exit(1)

logging.info("Main header path: " + MAIN_HEADER_PATH)
logging.info("External header path: " + EXTERNAL_HEADER_PATH)
logging.info("Main object path: " + MAIN_OBJ_PATH)
logging.info("External object path: " + EXTERNAL_OBJ_PATH)

shutil.rmtree('include', ignore_errors=True)
shutil.rmtree('objs', ignore_errors=True)

os.mkdir('include')
os.mkdir('objs')


def collect_internal_headers():
    filename_path_map = {}
    logging.info("Collecting headers...")
    collect_internal_headers_from_dir(MAIN_HEADER_PATH, filename_path_map)
    logging.info("Collected " + str(len(filename_path_map)) + " headers")

    logging.info("Copying headers...")
    # Copy the files to the include directory
    for filename, filepath in filename_path_map.items():
        shutil.copy(filepath, 'include/' + filename)
    logging.info("Copied " + str(len(filename_path_map)) + " headers")

    # Create a reverse map, remove path before 'src'
    path_filename_map = {}
    for filename, filepath in filename_path_map.items():
        rel_path = os.path.join(
            'src/', os.path.relpath(filepath, MAIN_HEADER_PATH))
        path_filename_map[rel_path] = filename
        logging.debug(rel_path + ' -> ' + filename)
    return path_filename_map


def collect_internal_headers_from_dir(dir, filename_path_map):
    for filepath in glob.iglob(dir + '**/**', recursive=True):
        if os.path.isfile(filepath) and filepath.endswith('.h') or filepath.endswith('.hpp'):
            filename = os.path.basename(filepath)
            i = 1
            # If there are multiple files with the same name, append a number to the end
            while filename in filename_path_map:
                extension = pathlib.Path(filepath).suffix
                filename_without_extension = pathlib.Path(filename).stem
                filename = filename_without_extension + \
                    '_' + str(i) + extension
                i += 1
            if i > 1:
                logging.warning('Duplicate header file name: ' +
                                filepath + ' -> ' + filename)
            filename_path_map[filename] = filepath
    return filename_path_map


def replace_include_paths_for_internal_headers(path_filename_map):
    logging.info("Replacing include paths...")
    for _, curr_header in path_filename_map.items():
        # Replace all include paths with the new path
        logging.debug("Replacing include paths in " + curr_header)
        with open('include/' + curr_header, 'r') as f:
            filedata = f.read()
        for path, filename in path_filename_map.items():
            if path in filedata:
                deployed_path = DEPLOYMENT_HEADER_PREFIX + filename
                filedata = filedata.replace(path, deployed_path)
                logging.debug("Replaced " + path + " -> " + deployed_path)
        with open('include/' + curr_header, 'w') as f:
            f.write(filedata)
    logging.info("Replaced include paths")


def check_for_unresolved_include_paths():
    logging.info("Checking for unresolved include paths...")
    unresolved_include_paths = {}
    for filepath in glob.iglob('include/*', recursive=True):
        if os.path.isfile(filepath) and filepath.endswith('.h') or filepath.endswith('.hpp'):
            with open(filepath, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if not (line.startswith('#include') and '"' in line):
                        continue
                    path = line.split('"')[1]
                    if os.path.exists('include/' + path):
                        continue
                    if path not in unresolved_include_paths:
                        unresolved_include_paths[path] = []
                    unresolved_include_paths[path].append(filepath)
    return unresolved_include_paths


def resolve_external_headers(unresolved_dict):
    all_unresolved_paths = list(unresolved_dict.keys())

    external_headers = []
    required_libs = []
    required_libs_name = []

    for filepath in glob.iglob(EXTERNAL_HEADER_PATH + '**/**', recursive=True):
        if os.path.isfile(filepath) and filepath.endswith('.h') or filepath.endswith('.hpp'):
            external_headers.append(filepath)
    logging.info("Found " + str(len(external_headers)) + " external headers")

    for unresolved_path in all_unresolved_paths:
        for e in external_headers:
            if unresolved_path in e:
                rel_path = os.path.relpath(e, EXTERNAL_HEADER_PATH)
                first_level_dir = Path(rel_path).parts[0]
                required_libs_name.append(first_level_dir)
                required_libs.append(
                    os.path.join(EXTERNAL_HEADER_PATH, first_level_dir))

    external_headers_to_copy = []
    for lib in required_libs:
        for h in external_headers:
            if lib in h:
                external_headers_to_copy.append(h)

    candidate_paths = []
    # Copy all external headers to the include directory
    logging.info("Copying required external headers...")
    for filepath in external_headers_to_copy:
        rel_path = os.path.relpath(filepath, EXTERNAL_HEADER_PATH)
        rel_path_dir = os.path.dirname(rel_path)
        os.makedirs(os.path.join('include/external/',
                    rel_path_dir), exist_ok=True)
        shutil.copy(filepath, os.path.join('include/external/', rel_path))
        candidate_paths.append(os.path.join('external/', rel_path))
    logging.info("Copied " + str(len(candidate_paths)) +
                 " external headers to include/external")

    for unresolved_path in all_unresolved_paths:
        for candidate_path in candidate_paths:
            if unresolved_path in candidate_path:
                logging.debug("Found " + unresolved_path +
                              " -> " + candidate_path)
                for f in unresolved_dict[unresolved_path]:
                    logging.debug("Replacing include path in " + f)
                    with open(f, 'r') as file:
                        filedata = file.read()
                    filedata = filedata.replace(
                        unresolved_path, candidate_path)
                    logging.debug("Replaced " + unresolved_path +
                                  " -> " + candidate_path)
                    with open(f, 'w') as file:
                        file.write(filedata)

    patch_spdlog()
    return required_libs_name


def patch_spdlog():
    # We manually override the include path for spdlog to use relative paths
    logging.info("Patching spdlog...")
    candidate_paths = [filepath for filepath in glob.iglob(
        'include/external/gabime_spdlog/**/**', recursive=True) if os.path.isfile(filepath) and filepath.endswith('.h') or filepath.endswith('.hpp')]

    for filepath in candidate_paths:
        if not os.path.isfile(filepath) and filepath.endswith('.h') or filepath.endswith('.hpp'):
            continue
        with open(filepath, 'r') as f:
            lines = f.readlines()
            f.seek(0)
            content = f.read()
        for line in lines:
            if not (line.startswith('#include') and '<' in line and "spdlog" in line):
                continue
            path = line.split('<')[1].split('>')[0]
            for c in candidate_paths:
                if path in c:
                    rel_path = os.path.relpath(c, os.path.dirname(filepath))
                    correct_include = '#include "%s"\n' % rel_path
                    logging.debug("Replaced " + line[:-1] +
                                  " in " + filepath + " -> " + correct_include[:-1])
                    content = content.replace(line, correct_include)
        with open(filepath, 'w') as f:
            f.write(content)
    logging.info("Patched spdlog")


def collect_obj_files_from_dir(dir, obj_map):
    for filepath in glob.iglob(dir + '**/**', recursive=True):
        if os.path.isfile(filepath) and filepath.endswith('.o'):
            md5sum = md5(open(filepath, 'rb').read()).hexdigest()
            if md5sum not in obj_map:
                obj_map[md5sum] = filepath


def collect_obj_files(external_lib_names):
    logging.info("Collecting object files...")
    paths_to_collect = [MAIN_OBJ_PATH]
    for lib in external_lib_names:
        paths_to_collect.append(os.path.join(EXTERNAL_OBJ_PATH, lib))
    obj_map = {}
    for path in paths_to_collect:
        collect_obj_files_from_dir(path, obj_map)
    logging.info("Collected " + str(len(obj_map)) + " object files")
    logging.info("Copying object files...")
    for md5sum in obj_map:
        shutil.copy(obj_map[md5sum], 'objs/' + md5sum + '.o')


def collect_executable():
    logging.info("Collecting executable...")
    dest = os.path.realpath(
        os.path.join('.', 'kuzu'))
    logging.debug("Copying executable: " + MAIN_EXE_PATH + " -> " + dest)
    shutil.copyfile(MAIN_EXE_PATH, dest)
    logging.debug("Fixing permissions for executable: " + dest)
    st = os.stat(dest)
    os.chmod(dest, st.st_mode | stat.S_IEXEC)
    logging.info("Copied executable")


def main():
    path_filename_map = collect_internal_headers()
    replace_include_paths_for_internal_headers(
        path_filename_map)
    unresolved_include_paths = check_for_unresolved_include_paths()
    if len(unresolved_include_paths) > 0:
        for path in unresolved_include_paths:
            logging.info("Unresolved path with internal headers: " + path + " in " +
                         str(unresolved_include_paths[path]))
        logging.info(
            "Trying to resolve the paths above with external headers...")
        external_lib_names = resolve_external_headers(unresolved_include_paths)
    unresolved_include_paths = check_for_unresolved_include_paths()
    if len(unresolved_include_paths) > 0:
        for path in unresolved_include_paths:
            logging.warning("Unresolved path with external headers: " + path + " in " +
                            str(unresolved_include_paths[path]))
    collect_obj_files(external_lib_names)
    collect_executable()


if __name__ == "__main__":
    main()
