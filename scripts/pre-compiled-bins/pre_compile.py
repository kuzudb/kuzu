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
if sys.platform == "linux":
    env_vars["CC"] = "gcc"
    env_vars["CXX"] = "g++"


def cleanup():
    logging.info("Cleaning up Bazel workspace...")
    subprocess.run(['bazel', 'clean'], cwd=bazel_workspace_root,
                   check=True, env=env_vars)
    logging.info("Bazel workspace cleaned up")


def build():
    args = ['--cxxopt=-std=c++2a', '--cxxopt=-O3']
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

        # It seems bazel does not automatically pick up
        # MACOSX_DEPLOYMENT_TARGETfrom the environment, so we need to pass
        # it explicitly.
        if "MACOSX_DEPLOYMENT_TARGET" in os.environ:
            args.append("--macos_minimum_os=" +
                        os.environ["MACOSX_DEPLOYMENT_TARGET"])

    full_cmd = ['bazel', 'build', *args, '//...:all']
    logging.info("Running command: %s" % full_cmd)
    subprocess.run(full_cmd, cwd=bazel_workspace_root,
                   check=True, env=env_vars)
    logging.info("Build completed")


def collect_files():
    logging.info("Running script to collect files...")
    subprocess.run([sys.executable, 'collect_files.py'],
                   cwd=base_dir)
    logging.info("Files collected")


def merge_libs():
    logging.info("Merging objects to shared lib...")
    cxx = "clang++" if sys.platform == "darwin" else "g++"
    output_path = os.path.realpath(os.path.join(base_dir, "libgraphflow.so"))
    input_files = []
    for root, _, files in os.walk(os.path.join(base_dir, "objs")):
        for file in files:
            if file.endswith(".o"):
                input_files.append(os.path.join(root, file))

    subprocess.run([cxx, "-shared", "-o", output_path, *input_files],
                   cwd=base_dir, env=env_vars, check=True)
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
