import argparse
import subprocess
import os
import shutil


def create_worktree(path, commit, repo_root):
    remove_worktree(path, repo_root)
    run_command(f"git worktree add {path} {commit}", cwd=repo_root)


def remove_worktree(path, repo_root):
    if os.path.exists(path):
        run_command(f"git worktree remove --force {path}", cwd=repo_root)


def check_for_extension_build(makefile):
    with open(makefile, "r") as f:
        return any(line.strip() == "extension-build:" for line in f)


# Duplicates code from benchmark/version.py.
# Should be fine since the footprint is small, but any further extensions
# to this tool should rework this if the footprint for duplicated code gets
# bigger.
def get_version(kuzu_root):
    cmake_file = os.path.join(kuzu_root, "CMakeLists.txt")
    with open(cmake_file) as f:
        for line in f:
            if line.startswith("project(Kuzu VERSION"):
                return line.split(" ")[2].strip()
    return "0"


def run_command(cmd, cwd=None, capture_output=False):
    print(f"> Running: {cmd} (cwd={cwd})")

    # We redirect stdin to devnull in an attempt
    # to stop the process from interfering with the terminal's input buffer.
    # This needs some work. After running the script I found that my buffer
    # was filled with a sequence like 8;1R8;1R8;1R8;1R8;... This doesn't seem
    # to affect the script but is annoying.
    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=capture_output,
        check=True,
        shell=True,
        stdin=subprocess.DEVNULL,
    )
    if capture_output:
        return result.stdout.strip()
    else:
        print(result.stdout or "")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Export DBs from dataset-dir to output-dir using base-commit and test in test-commit"
    )
    parser.add_argument(
        "--base-commit", required=True, help="Git commit to export databases from"
    )
    parser.add_argument(
        "--test-commit", required=True, help="Git commit to test against"
    )
    parser.add_argument(
        "--dataset-dir", required=True, help="Path to the dataset directory"
    )
    parser.add_argument(
        "--output-dir", required=True, help="Path to output the exported databases"
    )
    parser.add_argument(
        "--cleanup",
        dest="cleanup",
        action="store_true",
        help="Delete exported DBs after test",
    )
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help="Do not delete exported DBs after test",
    )
    parser.set_defaults(cleanup=True)
    args = parser.parse_args()

    base_commit = args.base_commit
    test_commit = args.test_commit
    dataset_dir = args.dataset_dir
    output_dir = args.output_dir
    cleanup = args.cleanup

    script_dir = os.path.dirname(os.path.realpath(__file__))
    kuzu_root = os.path.abspath(os.path.join(script_dir, ".."))
    base_worktree = os.path.join(kuzu_root, ".worktree-base")
    test_worktree = os.path.join(kuzu_root, ".worktree-test")
    create_worktree(base_worktree, base_commit, kuzu_root)
    create_worktree(test_worktree, test_commit, kuzu_root)
    export_path = None
    try:
        version = get_version(base_worktree)
        if version == "0":
            raise Exception("Failed to determine version. Aborting.")
        export_path = os.path.abspath(os.path.join(output_dir, version))

        if not os.path.exists(export_path + os.sep):
            # Some datasets, like tinysnb_json, have a dependency on the JSON
            # extension in their copy.cypher files. Therefore, we must build with
            # JSON support to ensure the export works correctly.

            # Older makefiles did not have the command specified under else
            if check_for_extension_build(
                os.path.abspath(os.path.join(base_worktree, "Makefile"))
            ):
                run_command(
                    "make extension-build EXTENSION_LIST=json", cwd=base_worktree
                )
            else:
                run_command(
                    "make extension-test-build EXTENSION_LIST=json", cwd=base_worktree
                )
            inprogress_path = f"{export_path}_inprogress" + os.sep
            export_script_path = os.path.join(kuzu_root, "scripts", "export-dbs.py")
            exec_path = os.path.join(
                base_worktree, "build", "relwithdebinfo", "tools", "shell", "kuzu"
            )
            run_command(
                f"""python3 {export_script_path} \
                --executable {exec_path} \
                --dataset-dir {dataset_dir} \
                --output-dir {inprogress_path}""",
                cwd=kuzu_root,
            )
            os.rename(inprogress_path, export_path + os.sep)

        # appends / so that datasets can be found correctly
        export_path += os.sep
        os.environ["E2E_IMPORT_DB_DIR"] = export_path
        run_command("make test", cwd=test_worktree)
        return 0
    finally:
        if cleanup and export_path and os.path.exists(export_path):
            print(f"Cleaning up export directory: {export_path}")
            shutil.rmtree(export_path)
        else:
            print(f"Skipping cleaning up export directory: {export_path}")

        print("Removing worktrees")
        remove_worktree(base_worktree, kuzu_root)
        remove_worktree(test_worktree, kuzu_root)


if __name__ == "__main__":
    main()
