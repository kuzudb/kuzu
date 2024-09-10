from __future__ import annotations

import platform
import urllib.request
import os
from pathlib import Path

import pytest
from type_aliases import ConnDB

EXTENSION_CMAKE_PREFIX = 'add_definitions(-DKUZU_EXTENSION_VERSION="'


@pytest.fixture()
def expected_extension_dir_prefix() -> str:
    system = platform.system()
    expected_extension_dir_prefix = None
    if system == "Windows":
        expected_extension_dir_prefix = "win_amd64"
    elif system == "Linux":
        expected_extension_dir_prefix = (
            "linux_arm64" if platform.machine() == "aarch64" or platform.machine() == "arm64" else "linux_amd64"
        )
    elif system == "Darwin":
        expected_extension_dir_prefix = "osx_arm64" if platform.machine() == "arm64" else "osx_amd64"
    return expected_extension_dir_prefix


def test_extension_install_httpfs(conn_db_readonly: ConnDB, tmpdir: str, expected_extension_dir_prefix: str) -> None:
    current_dir = Path(__file__).resolve().parent
    cmake_list_file = Path(current_dir).parent.parent.parent / "CMakeLists.txt"
    extension_version = None
    with Path.open(cmake_list_file) as f:
        for line in f:
            if EXTENSION_CMAKE_PREFIX in line:
                extension_version = line.split(EXTENSION_CMAKE_PREFIX)[1].split('"')[0]
                break
    userdir = os.path.expanduser("~")  # noqa: PTH111
    expected_path = (
        Path(userdir)
        .joinpath(".kuzu", "extension", extension_version, expected_extension_dir_prefix, "httpfs", "libhttpfs.kuzu_extension")
        .resolve()
    )
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-agent", "Kùzu Test Suite")]
    urllib.request.install_opener(opener)
    expected_download_url = f"http://extension.kuzudb.com/v{extension_version}/{expected_extension_dir_prefix}/httpfs/libhttpfs.kuzu_extension"
    urllib.request.urlretrieve(expected_download_url, Path(tmpdir) / "libhttpfs.kuzu_extension")

    conn, _ = conn_db_readonly
    conn.execute("INSTALL httpfs")

    assert Path.exists(expected_path)

    with Path.open(expected_path, "rb") as f:
        expected_content = f.read()
    with Path.open(Path(tmpdir) / "libhttpfs.kuzu_extension", "rb") as f:
        actual_content = f.read()
    assert expected_content == actual_content
