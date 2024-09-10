from __future__ import annotations

from type_aliases import ConnDB
import pytest
import platform
import os
import urllib.request

EXTENSION_CMAKE_PREFIX = "add_definitions(-DKUZU_EXTENSION_VERSION=\""

@pytest.fixture
def expected_extension_dir_prefix() -> str:
    system = platform.system()
    expected_extension_dir_prefix = None
    if system == "Windows":
        expected_extension_dir_prefix = "win_amd64"
    elif system == "Linux":
        if platform.machine() == 'aarch64' or platform.machine() == 'arm64':
            expected_extension_dir_prefix = "linux_arm64"
        else:
            expected_extension_dir_prefix = "linux_amd64"
    elif system == "Darwin":
        if platform.machine() == 'arm64':
            expected_extension_dir_prefix = "osx_arm64"
        else:
            expected_extension_dir_prefix = "osx_amd64"
    return expected_extension_dir_prefix

def test_extension_install_httpfs(conn_db_readonly: ConnDB, tmpdir: str, expected_extension_dir_prefix: str) -> None:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cmake_list_file = os.path.join(current_dir, "..", "..", "..", "CMakeLists.txt")
    extension_version = None
    with open(cmake_list_file, "r") as f:
        for line in f:
            if EXTENSION_CMAKE_PREFIX in line:
                extension_version = line.split(EXTENSION_CMAKE_PREFIX)[1].split("\"")[0]
                break
    userdir = os.path.expanduser("~")
    expected_path = os.path.abspath(os.path.join(userdir, ".kuzu", "extension", extension_version, expected_extension_dir_prefix, "httpfs", "libhttpfs.kuzu_extension"))
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-agent", "Kùzu Test Suite")]
    urllib.request.install_opener(opener)
    expected_download_url = f"http://extension.kuzudb.com/v{extension_version}/{expected_extension_dir_prefix}/httpfs/libhttpfs.kuzu_extension"
    urllib.request.urlretrieve(expected_download_url, os.path.join(tmpdir, "libhttpfs.kuzu_extension"))

    conn, _ = conn_db_readonly
    conn.execute("INSTALL httpfs")

    assert os.path.exists(expected_path)

    with open(expected_path, "rb") as f:
        expected_content = f.read()
    with open(os.path.join(tmpdir, "libhttpfs.kuzu_extension"), "rb") as f:
        actual_content = f.read()
    assert expected_content == actual_content
