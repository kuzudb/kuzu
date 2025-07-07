import sys
import argparse
import subprocess
import os
import shutil


def get_version(executable_path):
    try:
        result = subprocess.run(
            executable_path + " --version", capture_output=True, text=True, check=True
        )
        output = result.stdout.strip()
        if output.startswith("Kuzu "):
            return output.split(" ", 1)[1]
        else:
            print("Unexpected version format:", output)
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error running '{executable_path} --version': {e}")
        return None


def run_command(cmd, cwd=None, capture_output=False):
    if isinstance(cmd, str):
        cmd = cmd.split()

    print(f"> Running: {' '.join(cmd)} (cwd={cwd})")

    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=capture_output,
        check=True,
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
        type=bool,
        default=True,
        help="Delete exported DBs after test (default: True)",
    )
    args = parser.parse_args()

    base_commit = args.base_commit
    test_commit = args.test_commit
    dataset_dir = args.dataset_dir
    output_dir = args.output_dir
    cleanup = args.cleanup

    script_dir = os.path.dirname(os.path.realpath(__file__))
    kuzu_root = os.path.abspath(os.path.join(script_dir, ".."))
    current_branch = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=kuzu_root, capture_output=True
    )
    try:
        # Checkout commit A and build
        run_command("git checkout " + base_commit, cwd=kuzu_root)
        run_command("make extension-build EXTENSION_LIST=json", cwd=kuzu_root)

        # Switch back to working branch (to use the latest script)
        run_command("git checkout " + current_branch, cwd=kuzu_root)

        # Export databases
        export_script_path = os.path.join(kuzu_root, "scripts", "export-dbs.py")
        exec_path = os.path.join(
            kuzu_root, "build", "relwithdebinfo", "tools", "shell", "kuzu"
        )
        # Determine export path based on version (taken from export_script)
        version = get_version(exec_path)
        if version is None:
            raise Exception("Failed to determine version. Aborting.")
        export_path = os.path.join(output_dir, version) + os.sep

        # Only run export if export_path DNE
        if not os.path.exists(export_path):
            inprogress_path = export_path + "_inprogress" + os.sep
            run_command(
                "python3 "
                + export_script_path
                + " --executable "
                + exec_path
                + " --dataset-dir "
                + dataset_dir
                + " --output-dir "
                + inprogress_path,
                cwd=kuzu_root,
            )
            os.rename(inprogress_path, export_path)

        # Checkout commit B and run tests
        run_command("git checkout " + test_commit, cwd=kuzu_root)
        os.environ["E2E_IMPORT_DB_DIR"] = export_path
        run_command("make test", cwd=kuzu_root)

        # Restore original branch
        run_command("git checkout " + current_branch, cwd=kuzu_root)

        return 0
    finally:
        if cleanup:
            if os.path.exists(export_path):
                print(f"Cleaning up export directory: {export_path}")
                shutil.rmtree(export_path)
        else:
            print(f"Skipping cleaning up export directory: {export_path}")

        print(f"Restoring original git branch: {current_branch}")
        try:
            run_command("git checkout " + current_branch, cwd=kuzu_root)
        except Exception as e:
            print(f"Warning: Failed to restore branch {current_branch}: {e}")


if __name__ == "__main__":
    sys.exit(main())
