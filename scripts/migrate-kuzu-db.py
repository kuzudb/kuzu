#!/usr/bin/env python3
"""
Kuzu Database Migration Script

This script helps migrate Kuzu databases between versions.
- Sets up isolated Python environments for each Kuzu version
- Exports data from the source database using the old version
- Imports data into the target database using the new version
- If `overwrite` is enabled, the target database will replace the source database and the source database will be backed up with an `_old` suffix
- If `delete-old` is enabled, the source database will be deleted

Usage Examples:
    # Basic migration from 0.9.0 to 0.11.0
    python migrate-kuzu-db.py --old-version 0.9.0 --new-version 0.11.0 --old-db /path/to/old/database --new-db /path/to/new/database

Notes:
- Can only be used to migrate to newer Kuzu versions, from 0.11.0 onwards
"""

import tempfile
import sys
import struct
import shutil
import subprocess
import argparse
import os

# Database file extensions
KUZU_FILE_EXTENSIONS = ["", ".wal", ".shadow"]


# FIXME: Replace this with a Kuzu query to get the mapping when available.
kuzu_version_mapping = {
    34: "0.7.0",
    35: "0.7.1",
    36: "0.8.2",
    37: "0.9.0",
    38: "0.10.1",
    39: "0.11.0",
}

minimum_kuzu_migration_version = "0.11.0"


def kuzu_version_comparison(version: str, target: str) -> bool:
    """Return True if Kuzu *v* is greater or equal to target version"""
    # Transform version string to version tuple to use in version tuple comparison
    # NOTE: If version info contains non digit info (like dev release info 0.11.0.dev1) set the value of the non digit
    # tuple part to be 0 (transform it to 0.11.0.0)
    target = tuple(int(part) if part.isdigit() else 0 for part in target.split("."))
    current = tuple(int(part) if part.isdigit() else 0 for part in version.split("."))
    return current >= target


def read_kuzu_storage_version(kuzu_db_path: str) -> int:
    """
    Reads the Kuzu storage version.

    :param kuzu_db_path: Path to the Kuzu database file/directory.
    :return: Storage version code as an integer.
    """
    if os.path.isdir(kuzu_db_path):
        kuzu_version_file_path = os.path.join(kuzu_db_path, "catalog.kz")
        if not os.path.isfile(kuzu_version_file_path):
            raise FileNotFoundError("Kuzu catalog.kz file does not exist")
    else:
        kuzu_version_file_path = kuzu_db_path

    with open(kuzu_version_file_path, "rb") as f:
        f.seek(4)
        # Read the next 8 bytes as a little-endian unsigned 64-bit integer
        data = f.read(8)
        if len(data) < 8:
            raise ValueError(
                f"File '{kuzu_version_file_path}' does not contain a storage version code."
            )
        version_code = struct.unpack("<Q", data)[0]

    if version_code in kuzu_version_mapping:
        return kuzu_version_mapping[version_code]
    else:
        raise ValueError(f"Could not map version_code {version_code} to proper Kuzu version.")


def ensure_env(version: str, export_dir) -> str:
    """
    Creates a venv at `{export_dir}/.kuzu_envs/{version}` and installs `kuzu=={version}`
    Returns the venv's python executable path.
    """
    # Use temp directory to create venv
    kuzu_envs_dir = os.path.join(export_dir, ".kuzu_envs")

    # venv base under the script directory
    base = os.path.join(kuzu_envs_dir, version)
    py_bin = os.path.join(base, "bin", "python")
    # If environment already exists clean it
    if os.path.isdir(base):
        shutil.rmtree(base)

    print(f"→ Setting up venv for Kuzu {version}...", file=sys.stderr)
    # Create venv
    # NOTE: Running python in debug mode can cause issues with creating a virtual environment from that python instance
    subprocess.run([sys.executable, "-m", "venv", base], check=True)
    # Install the specific Kuzu version
    subprocess.run([py_bin, "-m", "pip", "install", "--upgrade", "pip"], check=True)
    subprocess.run([py_bin, "-m", "pip", "install", f"kuzu=={version}"], check=True)
    return py_bin


def run_migration_step(python_exe: str, db_path: str, cypher: str):
    """
    Uses `python_exe` to connect to the Kuzu database at `db_path` and run the `cypher` query.
    """
    snippet = f"""
import kuzu
db = kuzu.Database(r"{db_path}")
conn = kuzu.Connection(db)
conn.execute(r\"\"\"{cypher}\"\"\")
"""
    proc = subprocess.run([python_exe, "-c", snippet], capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"Error: query failed:\n{cypher}\n{proc.stderr}", file=sys.stderr)
        sys.exit(proc.returncode)


def kuzu_migration(
    new_db, old_db, new_version, old_version=None, overwrite=None, delete_old=None
):
    """
    Main migration function that handles the complete migration process.
    """
    if new_db == old_db:
        raise ValueError(
            "The new database path cannot be the same as the old database path. Please provide a different path for the new database."
        )

    if not kuzu_version_comparison(
        version=new_version, target=minimum_kuzu_migration_version
    ):
        raise ValueError(
            f"New version for kuzu is not supported, has to be equal or higher than version: {minimum_kuzu_migration_version}"
        )

    print(
        f"Migrating Kuzu database from {old_version} to {new_version}", file=sys.stderr
    )
    print(f"Source: {old_db}", file=sys.stderr)
    print("", file=sys.stderr)

    # If version of old kuzu db is not provided try to determine it based on file info
    if not old_version:
        old_version = read_kuzu_storage_version(old_db)

    # Check if old database exists
    if not os.path.exists(old_db):
        raise FileNotFoundError(f"Source database '{old_db}' does not exist.")

    # Prepare target - ensure parent directory exists but remove target if it exists
    parent_dir = os.path.dirname(new_db)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    if os.path.exists(new_db):
        raise FileExistsError(
            f"File already exists at {new_db}, remove file or change new database file path to continue"
        )

    # Use temp directory for all processing, it will be cleaned up after with statement
    with tempfile.TemporaryDirectory() as export_dir:
        # Set up environments
        print(f"Setting up Kuzu {old_version} environment...", file=sys.stderr)
        old_py = ensure_env(old_version, export_dir)
        print(f"Setting up Kuzu {new_version} environment...", file=sys.stderr)
        new_py = ensure_env(new_version, export_dir)

        export_file = os.path.join(export_dir, "kuzu_export")
        print(f"Exporting old DB → {export_dir}", file=sys.stderr)
        run_migration_step(old_py, old_db, f"EXPORT DATABASE '{export_file}'")
        print("Export complete.", file=sys.stderr)

        # Check if export files were created and have content
        schema_file = os.path.join(export_file, "schema.cypher")
        if not os.path.exists(schema_file) or os.path.getsize(schema_file) == 0:
            raise ValueError(f"Schema file not found: {schema_file}")

        print(f"Importing into new DB at {new_db}", file=sys.stderr)
        run_migration_step(new_py, new_db, f"IMPORT DATABASE '{export_file}'")
        print("Import complete.", file=sys.stderr)

    # Rename new kuzu database to old kuzu database name if enabled
    if overwrite or delete_old:
        # Remove kuzu lock from migrated DB
        lock_file = new_db + ".lock"
        if os.path.exists(lock_file):
            os.remove(lock_file)
        rename_databases(old_db, old_version, new_db, delete_old)

    print("Kuzu graph database migration finished successfully!")


def rename_databases(old_db: str, old_version: str, new_db: str, delete_old: bool):
    """
    When overwrite is enabled, back up the original old_db (file with .shadow and .wal or directory)
    by renaming it to *_old, and replace it with the newly imported new_db files.

    When delete_old is enabled, replace the old database with the new one and delete old database.

    :raises FileNotFoundError: If the original database path is not found
    :raises OSError: If file operations fail
    """
    base_dir = os.path.dirname(old_db)
    name = os.path.basename(old_db.rstrip(os.sep))
    # Add _old_ and version info to backup graph database
    backup_database_name = f"{name}_old_" + old_version.replace(".", "_")
    backup_base = os.path.join(base_dir, backup_database_name)

    if os.path.isfile(old_db):
        # File-based database: handle main file and accompanying lock/WAL
        for ext in KUZU_FILE_EXTENSIONS:
            src = old_db + ext
            dst = backup_base + ext
            if os.path.exists(src):
                if delete_old:
                    os.remove(src)
                else:
                    os.rename(src, dst)
                    print(f"Renamed '{src}' to '{dst}'", file=sys.stderr)
    elif os.path.isdir(old_db):
        # Directory-based Kuzu database
        backup_dir = backup_base
        if delete_old:
            shutil.rmtree(old_db)
        else:
            os.rename(old_db, backup_dir)
            print(f"Renamed directory '{old_db}' to '{backup_dir}'", file=sys.stderr)
    else:
        print(
            f"Original database path '{old_db}' not found for renaming.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Now move new files into place
    for ext in ["", ".wal", ".shadow"]:
        src_new = new_db + ext
        dst_new = os.path.join(base_dir, name + ext)
        if os.path.exists(src_new):
            os.rename(src_new, dst_new)
            print(f"Renamed '{src_new}' to '{dst_new}'", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(
        description="Migrate Kuzu DB via PyPI versions",
        epilog="""
Examples:
  %(prog)s --old-version 0.9.0 --new-version 0.11.0 \\
    --old-db /path/to/old/db --new-db /path/to/new/db --overwrite

Note: This script will create temporary virtual environments in .kuzu_envs/ directory
to isolate different Kuzu versions.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--old-version",
        required=False,
        default=None,
        help="Source Kuzu version (e.g., 0.9.0). If not provided, automatic kuzu version detection will be attempted.",
    )
    p.add_argument(
        "--new-version", required=True, help="Target Kuzu version (e.g., 0.11.0)"
    )
    p.add_argument("--old-db", required=True, help="Path to source database directory")
    p.add_argument(
        "--new-db",
        required=True,
        help="Path to target database directory, it can't be the same path as the old database. Use the overwrite flag if you want to replace the old database with the new one.",
    )
    p.add_argument(
        "--overwrite",
        required=False,
        action="store_true",
        default=False,
        help="Rename new-db to the old-db name and location, and keeps old-db as backup if delete-old is not True",
    )
    p.add_argument(
        "--delete-old",
        required=False,
        action="store_true",
        default=False,
        help="When overwrite and delete-old are True, old-db will not be stored as backup",
    )

    args = p.parse_args()

    kuzu_migration(
        new_db=args.new_db,
        old_db=args.old_db,
        new_version=args.new_version,
        old_version=args.old_version,
        overwrite=args.overwrite,
        delete_old=args.delete_old,
    )


if __name__ == "__main__":
    main()
