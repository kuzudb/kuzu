import os
import shutil
import sys
import platform
import requests

def extract_extension_version() -> str:
    import re
    pattern = r'-DKUZU_EXTENSION_VERSION=(["\'])(.*?)\1'
    with open('./CMakeLists.txt', 'r') as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                version = match.group(2)
                return version
    raise Exception("Failed to infer KUZU extension version from CMAKE file.")


def get_os() -> str:
    os_name = "linux"
    if sys.platform.startswith("win32"):
        os_name = "win"
    elif sys.platform.startswith("darwin"):
        os_name = "osx"
    return os_name


def get_arch() -> str:
    machine = platform.machine().lower()
    if "aarch64" in machine or "arm64" in machine:
        return "arm64"
    return "amd64"  # Default to amd64


extension_repo_path = 'extension/repo'
kuzu_version = 'v' + extract_extension_version()
arch_version = f"{get_os()}_{get_arch()}"
shutil.rmtree(extension_repo_path, ignore_errors=True)
os.mkdir(extension_repo_path)
path = os.path.join(extension_repo_path, kuzu_version)
os.mkdir(path)
path = os.path.join(path, arch_version)
os.mkdir(path)

# Create common dir
extensions = [d for d in os.listdir('extension') if os.path.isdir(os.path.join('extension', d)) and d != 'repo']
for extension in extensions:
    shutil.copytree(os.path.join('extension', extension, 'build'), os.path.join(path, extension))

# Download duckdb lib from official repo to local
if get_os() != "win":
    os.mkdir(os.path.join(path, "common"))
    file_name = 'libduckdb'
    if get_os() == "osx":
        file_name += '.dylib'
    else:
        file_name += '.so'
    official_repo = f"https://extension.kuzudb.com/{kuzu_version}/{arch_version}/common/{file_name}"
    response = requests.get(official_repo)
    if response.status_code == 200:
        with open(os.path.join(path, 'common', file_name), "wb") as f:
            f.write(response.content)
    else:
        print(official_repo)
        raise Exception("Failed to download shared duckdb library from official repo.")

os.system("python3 -m RangeHTTPServer 80")
