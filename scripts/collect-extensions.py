import os
import shutil
import platform

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
DST_DIR = os.path.abspath(os.path.join(FILE_DIR, "..", "extension-artifacts"))
SRC_DIR = os.path.abspath(os.path.join(FILE_DIR, "..", "extension"))


def collect_exts():
    for ext in os.listdir(SRC_DIR):
        ext_build_path = os.path.abspath(os.path.join(SRC_DIR, ext, "build"))
        if not os.path.exists(ext_build_path):
            continue
        print("Found extension: " + ext)
        ext_dst_path = os.path.abspath(os.path.join(DST_DIR, ext))
        os.makedirs(ext_dst_path, exist_ok=True)
        for f in os.listdir(ext_build_path):
            if not f.endswith(".kuzu_extension"):
                continue
            ext_file_path = os.path.abspath(os.path.join(ext_build_path, f))
            shutil.copy(ext_file_path, ext_dst_path)
            print(" \tCopied: " + f, "=>", ext_dst_path)


def find_duckdb():
    if platform.system() == "Darwin":
        candidates = [
            "/usr/local/lib/libduckdb.dylib",
            "/opt/homebrew/lib/libduckdb.dylib",
        ]
    elif platform.system() == "Linux":
        candidates = [
            "/usr/local/lib/libduckdb.so",
            "/usr/local/lib64/libduckdb.so",
            "/usr/lib/libduckdb.so",
            "/usr/lib64/libduckdb.so",
        ]
    elif platform.system() == "Windows":
        candidates = [
            "C:\\Program Files\\duckdb\\build\\release\\src\\Release\\duckdb.lib"
        ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return os.path.abspath(candidate)
    return None


def copy_duckdb():
    duckdb_dst_path = os.path.abspath(os.path.join(DST_DIR, "common"))
    os.makedirs(duckdb_dst_path, exist_ok=True)
    duckdb_path = find_duckdb()
    if duckdb_path is None:
        print("DuckDB not found, copying is skipped")
        return
    shutil.copy(duckdb_path, duckdb_dst_path)
    print("Copied DuckDB: " + duckdb_path, "=>", duckdb_dst_path)


def main():
    shutil.rmtree(DST_DIR, ignore_errors=True)
    os.makedirs(DST_DIR, exist_ok=True)
    collect_exts()
    copy_duckdb()


if __name__ == "__main__":
    main()
