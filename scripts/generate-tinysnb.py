import os
import shutil
import subprocess
import sys

KUZU_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
# Datasets can only be copied from the root since copy.schema contains relative paths
os.chdir(KUZU_ROOT)

# Define the build type from input
if len(sys.argv) > 1 and sys.argv[1].lower() == "release":
    build_type = "release"
else:
    build_type = "relwithdebinfo"

# Change the current working directory
if os.path.exists(f"{KUZU_ROOT}/dataset/databases/tinysnb"):
    shutil.rmtree(f"{KUZU_ROOT}/dataset/databases/tinysnb")
if sys.platform == "win32":
    kuzu_shell_path = f"{KUZU_ROOT}/build/{build_type}/src/kuzu_shell"
else:
    kuzu_shell_path = f"{KUZU_ROOT}/build/{build_type}/tools/shell/kuzu"
subprocess.check_call(
    [
        "python3",
        f"{KUZU_ROOT}/benchmark/serializer.py",
        "TinySNB",
        f"{KUZU_ROOT}/dataset/tinysnb",
        f"{KUZU_ROOT}/dataset/databases/tinysnb",
        "--single-thread",
        "--kuzu-shell",
        kuzu_shell_path,
    ]
)
