import argparse
import subprocess
import os
import shutil


def create_worktree(path, commit, repo_root):
    remove_worktree(path, repo_root)
    run_command(f"git worktree add {path} {commit}", cwd=repo_root)


def remove_worktree(path, repo_root):
    if os.path.exists(path):
        run_command(f"git worktree remove --force {path}", cwd=repo_root, check=False)


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


def run_command(cmd, cwd=None, capture_output=False, check=True):
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
        check=check,
        shell=True,
        stdin=subprocess.DEVNULL,
    )
    if capture_output:
        return result.stdout.strip()
    else:
        print(result.stdout or "")
        return None


def export_datasets_and_test(
    kuzu_root,
    base_worktree,
    test_worktree,
    dataset_dir,
    output_dir,
    cleanup,
    export_path,
):
    version = get_version(base_worktree)
    if version == "0":
        raise Exception("Failed to determine version. Aborting.")
    export_path = os.path.abspath(os.path.join(output_dir, version))

    if not os.path.exists(export_path + os.sep):
        # Also build the `json` extension, which is needed for some datasets, like tinysnb_json.
        if check_for_extension_build(
            os.path.abspath(os.path.join(base_worktree, "Makefile"))
        ):
            run_command("make extension-build EXTENSION_LIST=json", cwd=base_worktree)
        # Older Makefiles do not have the `extension-build` rule
        else:
            run_command(
                "make extension-test-build EXTENSION_LIST=json", cwd=base_worktree
            )

        # Use '_inprogress' as a temporary suffix to store the exports
        # and atomically rename to `export_path` after all exports are successful.
        # Avoids partial exports.
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

    # Append `/` so that datasets can be found correctly
    os.environ["E2E_IMPORT_DB_DIR"] = export_path + os.sep
    run_command("make test", cwd=test_worktree)
    return 0


def write_split_testfile(
    export_dir, import_dir, case_name, header, export_lines, import_lines, db_dir
):
    export_path = os.path.join(export_dir, f"{case_name}.test")
    import_path = os.path.join(import_dir, f"{case_name}.test")
    os.makedirs(export_dir, exist_ok=True)
    os.makedirs(import_dir, exist_ok=True)

    def replace_placeholders(lines):
        return [
            line.replace("${KUZU_EXPORT_DB_DIRECTORY}", os.path.join(db_dir, ""))
            for line in lines
        ]

    # Copying the dataset here is unnecessary as the tests use exported dbs.
    def transform_import_header(header):
        new_lines = []
        for line in header.splitlines(keepends=True):
            if line.startswith("-DATASET"):
                line = "-DATASET CSV empty\n"
            new_lines.append(line)
        return "".join(new_lines)

    # This is to handle an issue where the exported db seems to be
    # deleted making the import fail. DBs are still imported with the line
    # -STATEMENT IMPORT DATABASE ...
    def transform_import_lines(lines):
        result = []
        for line in lines:
            if line.startswith("-IMPORT_DATABASE"):
                continue
            result.append(line)
        return result

    with open(export_path, "w") as f:
        f.write(header.replace("${KUZU_EXPORT_DB_DIRECTORY}", os.path.join(db_dir, "")))
        f.writelines(replace_placeholders(export_lines))

    with open(import_path, "w") as f:
        f.write(
            transform_import_header(
                header.replace("${KUZU_EXPORT_DB_DIRECTORY}", os.path.join(db_dir, ""))
            )
        )
        f.writelines(transform_import_lines(replace_placeholders(import_lines)))


def split_tests(root, output_dir, file, db_dir):
    relative_path = os.path.relpath(file.name, root)
    base_path = os.path.splitext(relative_path)[0]
    export_dir = os.path.abspath(os.path.join(output_dir, "export", base_path))
    import_dir = os.path.abspath(os.path.join(output_dir, "import", base_path))

    header = ""
    header_parsed = False
    current_case = None
    export_lines = []
    import_lines = []
    in_case = False
    in_import = False
    for line in file:
        line = line.rstrip("\n")
        if not header_parsed:
            header += line + "\n"
            if line.strip() == "--":
                header_parsed = True
            continue
        if line.startswith("-CASE"):
            # Write the previous case, if it had a split.
            if in_case and in_import:
                write_split_testfile(
                    export_dir,
                    import_dir,
                    current_case,
                    header,
                    export_lines,
                    import_lines,
                    db_dir,
                )
            export_lines = []
            import_lines = []
            in_import = False
            current_case = line[len("-CASE") :].strip()
            in_case = True
            export_lines.append(line + "\n")
            import_lines.append(line + "\n")
            continue
        if line.startswith("#EXPORT_IMPORT_TEST_SPLIT"):
            in_import = True
            continue
        if in_case:
            if in_import:
                import_lines.append(line + "\n")
            else:
                export_lines.append(line + "\n")
    # Handle the last case of a file.
    if current_case and in_import:
        write_split_testfile(
            export_dir,
            import_dir,
            current_case,
            header,
            export_lines,
            import_lines,
            db_dir,
        )


def split_files(test_dir, output_dir):
    db_dir = os.path.abspath(os.path.join(output_dir, "db"))
    os.makedirs(db_dir, exist_ok=True)
    for root, dirs, files in os.walk(test_dir):
        for file in files:
            full_path = os.path.join(root, file)
            with open(full_path, "r") as f:
                split_tests(test_dir, output_dir, f, db_dir)


def run_export_specific_tests(
    kuzu_root, base_worktree, test_worktree, test_dir, output_dir, cleanup
):
    # Split tests in test_dir
    split_files(test_dir, output_dir)
    # Build base_worktree kuzu
    run_command("make test-build", cwd=base_worktree)
    # Run the export tests.
    run_command(
        f"E2E_TEST_FILES_DIRECTORY='.' ./.worktree-base/build/relwithdebinfo/test/runner/e2e_test {os.path.abspath(os.path.join(output_dir, 'export'))}",
        cwd=kuzu_root,
        check=False,
    )
    # Build test_worktree kuzu
    run_command("make test-build", cwd=test_worktree)
    # Run the import tests.
    run_command(
        f"E2E_TEST_FILES_DIRECTORY='.' ./.worktree-test/build/relwithdebinfo/test/runner/e2e_test {os.path.abspath(os.path.join(output_dir, 'import'))}",
        cwd=kuzu_root,
        check=False,
    )


def main():
    base_worktree = None
    test_worktree = None
    export_path = None
    cleanup = None

    parser = argparse.ArgumentParser(
        description="Export DBs from dataset-dir to output-dir using base-commit and test in test-commit"
    )
    parser.add_argument(
        "--base-commit", required=True, help="Git commit to export databases from"
    )
    parser.add_argument(
        "--test-commit", required=True, help="Git commit to test against"
    )

    parser.add_argument("--dataset-dir", help="Path to the dataset directory")
    parser.add_argument("--test-dir", help="Path to the test directory")

    parser.add_argument(
        "--output-dir", required=True, help="Path to output the exported databases"
    )

    mutually_exclusive_args = parser.add_mutually_exclusive_group()
    mutually_exclusive_args.add_argument(
        "--cleanup",
        dest="cleanup",
        action="store_true",
        help="Delete exported DBs after test",
    )
    mutually_exclusive_args.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help="Do not delete exported DBs after test",
    )
    parser.set_defaults(cleanup=True)

    try:
        args = parser.parse_args()

        if bool(args.dataset_dir) == bool(args.test_dir):
            raise Exception(
                "You must provide exactly one of --dataset-dir or --test-dir."
            )

        base_commit = args.base_commit
        test_commit = args.test_commit
        output_dir = args.output_dir
        cleanup = args.cleanup

        script_dir = os.path.dirname(os.path.realpath(__file__))
        kuzu_root = os.path.abspath(os.path.join(script_dir, ".."))
        base_worktree = os.path.join(kuzu_root, ".worktree-base")
        test_worktree = os.path.join(kuzu_root, ".worktree-test")
        create_worktree(base_worktree, base_commit, kuzu_root)
        create_worktree(test_worktree, test_commit, kuzu_root)

        if bool(args.dataset_dir):
            export_datasets_and_test(
                kuzu_root,
                base_worktree,
                test_worktree,
                os.path.abspath(args.dataset_dir),
                output_dir,
                cleanup,
                export_path,
            )
        else:
            assert bool(args.test_dir)
            export_path = output_dir
            run_export_specific_tests(
                kuzu_root,
                base_worktree,
                test_worktree,
                os.path.abspath(args.test_dir),
                output_dir,
                cleanup,
            )

    finally:
        if cleanup and export_path and os.path.exists(export_path):
            print(f"Cleaning up export directory: {export_path}")
            shutil.rmtree(export_path)
        else:
            print(f"Skipping cleaning up export directory: {export_path}")

        if base_worktree or test_worktree:
            print("Removing worktrees")
            if base_worktree:
                remove_worktree(base_worktree, kuzu_root)
            if test_worktree:
                remove_worktree(test_worktree, kuzu_root)


if __name__ == "__main__":
    main()
