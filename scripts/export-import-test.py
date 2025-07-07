import argparse
import subprocess
import os
import shutil


def run_command(cmd, cwd=None, capture_output=False):
    print(f"> Running: {cmd} (cwd={cwd})")

    # We redirect stdin to devnull in an attempt
    # to stop the proccess from intefering with the terminal's input buffer.
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
        "git rev-parse --abbrev-ref HEAD", cwd=kuzu_root, capture_output=True
    )
    try:
        run_command(f"git checkout {base_commit}", cwd=kuzu_root)

        version = run_command(f"python3 benchmark/version.py", cwd=kuzu_root, capture_output=True)
        if version == "0":
            raise Exception("Failed to determine version. Aborting.")
        export_path = os.path.join(output_dir, version)

        if not os.path.exists(export_path + os.sep):
            export_script_path = os.path.join(kuzu_root, "scripts", "export-dbs.py")
            exec_path = os.path.join(
                kuzu_root, "build", "relwithdebinfo", "tools", "shell", "kuzu"
            )
            # Some datasets, like tinysnb_json, have a dependency on the JSON
            # extension in their copy.cypher files. Therefore, we must build with
            # JSON support to ensure the export works correctly.
            run_command("make extension-build EXTENSION_LIST=json", cwd=kuzu_root)
            inprogress_path = f"{export_path}_inprogress" + os.sep
            run_command(f"git checkout {current_branch}", cwd=kuzu_root)
            run_command(
                f"""python3 {export_script_path} \
                --executable {exec_path} \
                --dataset-dir {dataset_dir} \
                --output-dir {inprogress_path}""",
                cwd=kuzu_root,
            )
            os.rename(inprogress_path, export_path + os.sep)
        # Switch back to working branch (to use the latest script)
        run_command(f"git checkout {current_branch}", cwd=kuzu_root)

        # Checkout commit B and run tests
        run_command(f"git checkout {test_commit}", cwd=kuzu_root)
        # appends / so that datasets can be found correctly
        export_path += os.sep
        os.environ["E2E_IMPORT_DB_DIR"] = export_path
        run_command("make test", cwd=kuzu_root)

        # Restore original branch
        run_command(f"git checkout {current_branch}", cwd=kuzu_root)

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
            run_command(f"git checkout {current_branch}", cwd=kuzu_root)
        except Exception as e:
            print(f"Warning: Failed to restore branch {current_branch}: {e}")


if __name__ == "__main__":
    main()
