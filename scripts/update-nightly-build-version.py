PYPI_URL = "https://pypi.org/pypi/kuzu/json"
CMAKE_KEYWORD = "project(Kuzu VERSION "
CMAKE_SUFFIX = " LANGUAGES CXX C)\n"
EXTENSION_KEYWORD = 'add_definitions(-DKUZU_EXTENSION_VERSION="'
EXTENSION_SUFFIX = '")\n'
EXTENSION_DEV_VERSION = "dev"

import urllib.request
import json
import os
import sys

def main():
    try:
        from packaging.version import Version
    except ImportError:
        from distutils.version import LooseVersion as Version
    with urllib.request.urlopen(PYPI_URL) as url:
        data = json.loads(url.read().decode())
    releases = data["releases"]
    versions = list(releases.keys())
    versions.sort(key=Version)
    dev_versions = [v for v in versions if "dev" in v]
    stable_versions = [v for v in versions if not "dev" in v]
    latest_dev_version = dev_versions[-1] if len(dev_versions) > 0 else None
    latest_stable_version = stable_versions[-1] if len(stable_versions) > 0 else None
    print("Latest dev version: %s." % str(latest_dev_version))
    print("Latest stable version: %s." % str(latest_stable_version))
    if latest_dev_version is None and latest_stable_version is None:
        print("No versions found. Defaulting to 0.0.1.dev1.")
        dev_version = "0.0.1.dev1"
    elif latest_dev_version is None or Version(latest_dev_version) < Version(latest_stable_version):
        print("The latest stable version is newer than dev version or no dev version exists. Bumping dev version from stable version.")
        latest_stable_version_split = latest_stable_version.split(".")
        latest_stable_version_split[-1] = str(int(latest_stable_version_split[-1]) + 1)
        latest_stable_version_split.append("dev1")
        dev_version = ".".join(latest_stable_version_split)
    else:
        print("The latest dev version is newer than stable version. Bumping dev version from dev version.")
        latest_dev_version_split = latest_dev_version.split(".")
        latest_dev_version_split[-1] = "dev" + str(int(latest_dev_version_split[-1][3:]) + 1)
        dev_version = ".".join(latest_dev_version_split)
    print("New Python dev version: %s." % dev_version)
    cmake_version = dev_version.replace("dev", "")
    print("New CMake version: %s." % cmake_version)
    cmake_lists_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "CMakeLists.txt"))
    print("Updating %s..." % cmake_lists_path)
    with open(cmake_lists_path, "r") as cmake_lists_file:
        cmake_lists = cmake_lists_file.readlines()
    counter = 2
    for i, line in enumerate(cmake_lists):
        if CMAKE_KEYWORD in line:
            cmake_lists[i] = CMAKE_KEYWORD + cmake_version + CMAKE_SUFFIX
            counter -= 1
        if EXTENSION_KEYWORD in line:
            cmake_lists[i] = EXTENSION_KEYWORD + EXTENSION_DEV_VERSION + EXTENSION_SUFFIX
            counter -= 1
        if counter == 0:
            break
    with open(cmake_lists_path, "w") as cmake_lists_file:
        cmake_lists_file.writelines(cmake_lists)
    print("Committing changes...")
    sys.stdout.flush()
    os.system("git config user.email ci@kuzudb.com")
    os.system("git config user.name \"Kuzu CI\"")
    os.system("git add %s" % cmake_lists_path)
    os.system("git commit -m \"Update CMake version to %s and change extension version to dev.\"" % cmake_version)
    sys.stdout.flush()
    sys.stderr.flush()
    print("All done!")
    sys.stdout.flush()

if __name__ == "__main__":
    main()
