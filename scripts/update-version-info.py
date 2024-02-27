import os


def main():
    storage_info_path = os.path.join(
        os.path.dirname(__file__), "..", "src", "include", "storage", "storage_info.h"
    )
    version_info = eval(
        open(storage_info_path)
        .read()
        .split("getStorageVersionInfo()")[1]
        .split("static storage_version_t")[0]
        .strip()
        .replace("return", "")
        .replace("{", "(")
        .replace("}", ")")
        .replace(";", "")
        .strip()
    )
    version_info_dict = {}
    for ver, storage_ver in version_info:
        version_info_dict[ver] = storage_ver
    cmake_lists_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "CMakeLists.txt")
    )
    cmake_version = (
        open(cmake_lists_path)
        .read()
        .split("project(Kuzu VERSION ")[1]
        .split(")")[0]
        .strip()
        .split(" ")[0]
        .strip()
    )
    if cmake_version in version_info_dict:
        storage_version = version_info_dict[cmake_version]
    else:
        # If the version is not found, it means that the version is a dev version,
        # so we default to the latest storage version
        storage_version = max(version_info_dict.values())
    print("CMake version: %s" % cmake_version)
    print("Storage version: %d" % storage_version)


if __name__ == "__main__":
    main()
