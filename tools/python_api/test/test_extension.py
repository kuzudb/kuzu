from __future__ import annotations

import hashlib
import os
import platform
import urllib.request
from pathlib import Path

import pytest
from type_aliases import ConnDB

EXTENSION_CMAKE_PREFIX = 'add_definitions(-DKUZU_EXTENSION_VERSION="'


@pytest.fixture()
def extension_extension_dir_prefix() -> str:
    system = platform.system()
    extension_extension_dir_prefix = None
    if system == "Windows":
        extension_extension_dir_prefix = "win_amd64"
    elif system == "Linux":
        extension_extension_dir_prefix = (
            "linux_arm64" if platform.machine() == "aarch64" or platform.machine() == "arm64" else "linux_amd64"
        )
    elif system == "Darwin":
        extension_extension_dir_prefix = "osx_arm64" if platform.machine() == "arm64" else "osx_amd64"
    return extension_extension_dir_prefix


def test_extension_install_httpfs(conn_db_readonly: ConnDB, tmpdir: str, extension_extension_dir_prefix: str) -> None:
    current_dir = Path(__file__).resolve().parent
    cmake_list_file = Path(current_dir).parent.parent.parent / "CMakeLists.txt"
    extension_version = None
    with Path.open(cmake_list_file) as f:
        for line in f:
            if EXTENSION_CMAKE_PREFIX in line:
                extension_version = line.split(EXTENSION_CMAKE_PREFIX)[1].split('"')[0]
                break
    userdir = os.path.expanduser("~")  # noqa: PTH111
    extension_path = (
        Path(userdir)
        .joinpath(
            ".kuzu",
            "extension",
            extension_version,
            extension_extension_dir_prefix,
            "httpfs",
            "libhttpfs.kuzu_extension",
        )
        .resolve()
    )
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-agent", "Kùzu Test Suite")]
    urllib.request.install_opener(opener)
    download_url = f"http://extension.kuzudb.com/v{extension_version}/{extension_extension_dir_prefix}/httpfs/libhttpfs.kuzu_extension"
    temp_path = Path(tmpdir) / "libhttpfs.kuzu_extension"
    urllib.request.urlretrieve(download_url, temp_path)

    conn, _ = conn_db_readonly
    conn.execute("INSTALL httpfs")

    assert Path.exists(extension_path)

    extension_size = Path(extension_path).stat().st_size
    expected_size = Path(temp_path).stat().st_size

    with Path.open(extension_path, "rb") as f:
        extension_md5 = hashlib.md5(f.read()).hexdigest()
    with Path.open(temp_path, "rb") as f:
        expected_md5 = hashlib.md5(f.read()).hexdigest()

    assert extension_size == expected_size
    assert extension_md5 == expected_md5
