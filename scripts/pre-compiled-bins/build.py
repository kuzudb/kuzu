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

    full_cmd = ['make', 'release', 'NUM_THREADS=%d' % concurrency]
    logging.info("Running command: %s" % full_cmd)
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


def collect_binaries():
    logging.info("Collecting binaries...")
    c_header_path = os.path.join(workspace_root, "src", "include", "c_api",
                                 "kuzu.h")
    so_path = os.path.join(workspace_root, "build",
                           "release", "src", "libkuzu.so")
    dylib_path = os.path.join(workspace_root, "build",
                              "release", "src", "libkuzu.dylib")
    if not os.path.exists(c_header_path):
        logging.error("No C header file found")
        sys.exit(1)
    shutil.copy(c_header_path, os.path.join(base_dir, "kuzu.h"))
    logging.info("Copied kuzu.h")
    if sys.platform == "win32":
        dll_path = os.path.join(workspace_root, "build",
                                "release", "src", "kuzu_shared.dll")
        lib_path = os.path.join(workspace_root, "build",
                                "release", "src", "kuzu_shared.lib")
        if not os.path.exists(dll_path):
            logging.error("No dll found")
            sys.exit(1)
        if not os.path.exists(lib_path):
            logging.error("No import library found")
            sys.exit(1)
        shutil.copy(dll_path, os.path.join(base_dir, "kuzu_shared.dll"))
        logging.info("Copied kuzu_shared.dll")
        shutil.copy(lib_path, os.path.join(base_dir, "kuzu_shared.lib"))
        logging.info("Copied kuzu_shared.lib")
        shell_path = os.path.join(workspace_root, "build", "release",
                                  "tools", "shell", "kuzu_shell.exe")
        if not os.path.exists(shell_path):
            logging.error("No shell binary found")
            sys.exit(1)
        shutil.copy(shell_path, os.path.join(base_dir, "kuzu.exe"))
    else:
        so_exists = os.path.exists(so_path)
        dylib_exists = os.path.exists(dylib_path)
        if not so_exists and not dylib_exists:
            logging.error("No shared object file found")
            sys.exit(1)
        if so_exists:
            shutil.copy(so_path, os.path.join(base_dir, "libkuzu.so"))
            logging.info("Copied libkuzu.so")
        if dylib_exists:
            shutil.copy(dylib_path, os.path.join(base_dir, "libkuzu.dylib"))
            logging.info("Copied libkuzu.so")
        shell_path = os.path.join(workspace_root, "build",
                                  "release", "tools", "shell", "kuzu_shell")
        if not os.path.exists(shell_path):
            logging.error("No shell binary found")
            sys.exit(1)
        shutil.copy(shell_path, os.path.join(base_dir, "kuzu"))
    logging.info("Copied kuzu")
    logging.info("Binaries collected")


def main():
    cleanup()
    build()
    collect_binaries()
    collect_and_merge_headers()


if __name__ == "__main__":
    main()
