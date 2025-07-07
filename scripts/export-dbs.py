import argparse
import os
import sys
import subprocess


# Parse schema.cypher and copy.cypher files.
def create_cypher_queries(file_path):
    commands = []
    try:
        with open(file_path, "r") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                if not stripped.endswith(";"):
                    stripped += ";"
                commands.append(stripped)
    except Exception:
        pass
    return commands


# Find all datasets that have a schema.cypher and copy.cypher file.
def find_valid_dataset_dirs(dataset_root):
    valid_dirs = []

    for root, dirs, files in os.walk(dataset_root):
        # This script creates a tmp directory with the exported dbs, we should
        # skip it in our search.
        if "tmp" in root.split(os.sep):
            continue
        file_set = set(files)

        if "schema.cypher" in file_set:
            valid_dirs.append(root)

    return valid_dirs


# Example scripts/export-dbs.py build/debug/tools/shell/kuzu dataset.
def main():
    parser = argparse.ArgumentParser(
        description="Export DBs with KUZU shell and dataset paths"
    )

    parser.add_argument(
        "--executable", required=True, help="Path to the KUZU shell executable"
    )
    parser.add_argument(
        "--dataset-dir", required=True, help="Path to the dataset directory"
    )
    parser.add_argument(
        "--output-dir", required=True, help="Path to export the datasets"
    )
    args = parser.parse_args()

    arg_executable_path = os.path.abspath(args.executable)
    arg_dataset_path = os.path.abspath(args.dataset_dir)
    output_dir = os.path.abspath(args.output_dir)

    if not os.path.isfile(arg_executable_path):
        raise Exception(f"Error: Executable not found at {arg_executable_path}")
    if not os.path.exists(arg_dataset_path):
        raise Exception(f"Error: Dataset path not found at {arg_dataset_path}")

    valid_datasets = find_valid_dataset_dirs(arg_dataset_path)
    # This is done to construct a full path to replace the relative paths found
    # in copy.cypher files.
    script_dir = os.path.dirname(os.path.realpath(__file__))
    root_dir = os.path.abspath(os.path.join(script_dir, ".."))
    for dataset_path in valid_datasets:
        schema_commands = create_cypher_queries(
            os.path.join(dataset_path, "schema.cypher")
        )
        copy_commands = create_cypher_queries(os.path.join(dataset_path, "copy.cypher"))
        combined_commands = schema_commands + copy_commands
        dataset_name = os.path.relpath(dataset_path, arg_dataset_path)
        export_path = os.path.join(output_dir, dataset_name)
        export_command = (
            f"EXPORT DATABASE '{export_path}' (format=\"csv\", header=true);"
        )
        combined_commands.append(export_command)
        combined_commands.insert(0, "CALL threads=1;")
        print(f"Exporting {dataset_path} to {export_path}")
        joined_commands = "\n".join(cmd.strip() for cmd in combined_commands)

        subprocess.run(
            arg_executable_path,
            input=joined_commands,
            text=True,
            cwd=root_dir,
            check=True,
            shell=True,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
