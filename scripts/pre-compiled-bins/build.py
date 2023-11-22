import subprocess
import os
import sys
import logging
import shutil
import multiprocessing

concurrency = multiprocessing.cpu_count()

logging.basicConfig(level=logging.DEBUG)

base_dir = os.path.dirname(__file__)
workspace_root = os.path.realpath(os.path.join(base_dir, "..", ".."))
env_vars = os.environ.copy()
env_vars['PYTHON_BIN_PATH'] = sys.executable
if sys.platform == "linux":
    env_vars["CC"] = "gcc"
    env_vars["CXX"] = "g++"
elif sys.platform == "darwin":
    env_vars["CC"] = "clang"
    env_vars["CXX"] = "clang++"


def cleanup():
    logging.info("Cleaning up workspace...")
    try:
        subprocess.run(['make', 'clean-all'], cwd=workspace_root,
                       check=True, env=env_vars)
        shutil.rmtree(os.path.join(base_dir, "headers"), ignore_errors=True)
        for f in ["kuzu", "kuzu.hpp", "kuzu.h", "libkuzu.so", "libkuzu.dylib"]:
            try:
                os.remove(os.path.join(base_dir, f))
            except OSError:
                pass
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)
    logging.info("Workspace cleaned up")


def build():
    logging.info("Building kuzu...")
    if sys.platform == 'darwin':
        archflags = os.getenv("ARCHFLAGS", "")

        if len(archflags) > 0:
            logging.info("The ARCHFLAGS is set to '%s'." %
                         archflags)
            if archflags == "-arch arm64":
                env_vars['CMAKE_OSX_ARCHITECTURES'] = "arm64"
            elif archflags == "-arch x86_64":
                env_vars['CMAKE_OSX_ARCHITECTURES'] = "x86_64"
            else:
                logging.info(
                    "The ARCHFLAGS is not valid and will be ignored.")
        else:
            logging.info("The ARCHFLAGS is not set.")

        deploy_target = os.getenv("MACOSX_DEPLOYMENT_TARGET", "")
        if len(deploy_target) > 0:
            logging.info("The deployment target is set to '%s'." %
                         deploy_target)
            env_vars['CMAKE_OSX_DEPLOYMENT_TARGET'] = deploy_target

    full_cmd = ['make', 'release', 'LTO=1', 'NUM_THREADS=%d' % concurrency]
    logging.info("Running command: %s" % full_cmd)
    try:
        subprocess.run(full_cmd, cwd=workspace_root,
                       check=True, env=env_vars)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)

    full_cmd = ['make', 'install']
    logging.info("Stripping kuzu binaries...")
    try:
        subprocess.run(full_cmd, cwd=workspace_root,
                       check=True, env=env_vars)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)

    logging.info("Build completed")


def collect_and_merge_headers():
    logging.info("Running script to collect and merge headers...")
    try:
        subprocess.run([sys.executable, 'collect_files.py'],
                       cwd=base_dir)
        subprocess.run([sys.executable, 'merge_headers.py'],
                       cwd=base_dir)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)
    logging.info("Files collected")


def quit_with(msg):
    logging.error(msg)
    sys.exit(1)

def assert_exists(path, msg):
    if not os.path.exists(path):
        quit_with(msg)

def find_stripped_library(filename):
    install_dir = os.path.join(workspace_root, "install")
    for lib_dir in ["lib", "lib64"]:
        path = os.path.join(install_dir, lib_dir, filename)
        if os.path.exists(path):
            return path

    return None

def find_stripped_library_or(filename, msg):
    path = find_stripped_library(filename)
    if path is None:
        quit_with(msg)
    return path

def find_stripped_binary_or(filename, msg):
    path = os.path.join(workspace_root, "install", "bin", filename)
    assert_exists(path, msg)
    return path

def copy_and_log(path, filename):
    shutil.copy(path, os.path.join(base_dir, filename))
    logging.info(f"Copied {filename}")

def collect_binaries():
    logging.info("Collecting binaries...")
    c_header_path = os.path.join(workspace_root, "src", "include", "c_api",
                                 "kuzu.h")
    assert_exists(c_header_path, "No C header file found")
    shutil.copy(c_header_path, os.path.join(base_dir, "kuzu.h"))
    logging.info("Copied kuzu.h")

    if sys.platform == "win32":
        dll_path = find_stripped_binary_or("kuzu_shared.dll", "No dll found")
        lib_path = find_stripped_library_or("kuzu_shared.lib", "No import library found")
        shell_path = find_stripped_binary_or("kuzu_shell.exe", "No shell binary found")

        copy_and_log(dll_path, "kuzu_shared.dll")
        copy_and_log(lib_path, "kuzu_shared.lib")
        copy_and_log(shell_path, "kuzu.exe")
    else:
        so_path = find_stripped_library("libkuzu.so")
        dylib_path = find_stripped_library("libkuzu.dylib")
        if so_path is not None:
            copy_and_log(so_path, "libkuzu.so")
        elif dylib_path is not None:
            copy_and_log(dylib_path, "libkuzu.dylib")
        else:
            quit_with("No shared object file found")

        shell_path = find_stripped_binary_or("kuzu_shell", "No shell binary found")
        copy_and_log(shell_path, "kuzu")
    logging.info("Binaries collected")


def main():
    cleanup()
    build()
    collect_binaries()
    collect_and_merge_headers()


if __name__ == "__main__":
    main()
