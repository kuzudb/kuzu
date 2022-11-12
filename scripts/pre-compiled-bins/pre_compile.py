import subprocess
import os
import sys
import logging
import platform
import shutil

logging.basicConfig(level=logging.DEBUG)

base_dir = os.path.dirname(__file__)
bazel_workspace_root = os.path.realpath(os.path.join(base_dir, "..", ".."))
env_vars = os.environ.copy()
env_vars['PYTHON_BIN_PATH'] = sys.executable
if sys.platform == "linux":
    env_vars["CC"] = "gcc"
    env_vars["CXX"] = "g++"


def cleanup():
    logging.info("Cleaning up Bazel workspace...")
    try:
        subprocess.run(['bazel', 'clean'], cwd=bazel_workspace_root,
                       check=True, env=env_vars)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)
    logging.info("Bazel workspace cleaned up")


def build():
    args = ['--cxxopt=-std=c++2a', '--cxxopt=-O3', '--cxxopt=-fPIC',
            '--cxxopt=-DNDEBUG']
    if sys.platform == 'darwin':
        archflags = os.getenv("ARCHFLAGS", "")
        if len(archflags) > 0:
            logging.info("The ARCHFLAGS is set to '%s' for macOS" %
                         archflags)
        else:
            logging.info(
                "The ARCHFLAGS is not set for macOS, will default to machine architecture")
        if "arm64" in archflags and platform.machine() == "x86_64":
            args.append("--macos_cpus=arm64")
            args.append("--cpu=darwin_arm64")
            args.append('--cxxopt=--target=arm64-apple-darwin')

        # It seems bazel does not automatically pick up
        # MACOSX_DEPLOYMENT_TARGETfrom the environment, so we need to pass
        # it explicitly.
        if "MACOSX_DEPLOYMENT_TARGET" in os.environ:
            args.append("--macos_minimum_os=" +
                        os.environ["MACOSX_DEPLOYMENT_TARGET"])
    elif sys.platform == "linux":
        args.append('--cxxopt=-D_GLIBCXX_USE_CXX11_ABI=1')
    full_cmd = ['bazel', 'build', *args, '//...:all']
    logging.info("Running command: %s" % full_cmd)
    try:
        subprocess.run(full_cmd, cwd=bazel_workspace_root,
                       check=True, env=env_vars)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)
    logging.info("Build completed")


def collect_files():
    logging.info("Running script to collect files...")
    try:
        subprocess.run([sys.executable, 'collect_files.py'],
                       cwd=base_dir)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)
    logging.info("Files collected")


def merge_libs():
    logging.info("Merging objects to shared lib...")

    if sys.platform == 'darwin':
        cxx = "clang++"
        output_name = "libkuzu.dylib"
        archflags = os.getenv("ARCHFLAGS", "")
        if "arm64" in archflags and platform.machine() == "x86_64":
            additional_args = ["--target=arm64-apple-darwin"]
        else:
            additional_args = []
        linker_args = ["-dynamiclib", "-undefined", "dynamic_lookup",
                       "-Wl,-install_name,@rpath/libkuzu.dylib"]
    elif sys.platform == "linux":
        cxx = "g++"
        output_name = "libkuzu.so"
        additional_args = []
        linker_args = ["-shared", "-fPIC", "-Wl,-soname,libkuzu.so"]

    output_path = os.path.realpath(os.path.join(base_dir, output_name))
    input_files = []
    for root, _, files in os.walk(os.path.join(base_dir, "objs")):
        for file in files:
            if file.endswith(".o"):
                input_files.append(os.path.join(root, file))

    cxx_cmd = [cxx, *additional_args, *linker_args, "-o", output_path]
    logging.debug("Running command: %s (input objects truncated)..." % cxx_cmd)
    cxx_cmd.extend(input_files)
    try:
        subprocess.run(cxx_cmd, cwd=base_dir, env=env_vars, check=True)
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        logging.error("Failed with return code %d" % return_code)
        sys.exit(1)
    logging.info("Shared lib merged")


def remove_objs():
    logging.info("Removing object files...")
    shutil.rmtree(os.path.join(base_dir, "objs"))
    logging.info("Object files removed")


def main():
    cleanup()
    build()
    collect_files()
    merge_libs()
    remove_objs()


if __name__ == "__main__":
    main()
