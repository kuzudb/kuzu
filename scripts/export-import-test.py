import sys
import argparse
import subprocess
import os


def getVersion(executablePath):
    try:
        result = subprocess.run(
            [executablePath, "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout.strip()
        if output.startswith("Kuzu "):
            return output.split(" ", 1)[1]
        else:
            print("Unexpected version format:", output)
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error running '{executablePath} --version': {e}")
        return None


def runCommand(cmd, cwd=None, env=None):
    if isinstance(cmd, str):
        cmd = cmd.split()

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed: {' '.join(cmd)}\n{e.stderr}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Export DBs from datasetDir using commitA and test in commitB"
    )
    parser.add_argument("commitA", help="Git commit to export databases from")
    parser.add_argument("commitB", help="Git commit to test against")
    parser.add_argument("datasetDir", help="Path to the dataset directory")
    args = parser.parse_args()

    commitA = args.commitA
    commitB = args.commitB
    datasetDir = args.datasetDir

    scriptDir = os.path.dirname(os.path.realpath(__file__))
    kuzuRoot = os.path.abspath(os.path.join(scriptDir, ".."))
    currentBranch = runCommand(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=kuzuRoot)

    # Checkout commit A and build
    runCommand(["git", "checkout", commitA], cwd=kuzuRoot)
    runCommand(["make", "extension-build", "EXTENSION_LIST=json"], cwd=kuzuRoot)

    # Switch back to working branch (to use the following script)
    runCommand(["git", "checkout", currentBranch], cwd=kuzuRoot)

    # Export databases
    exportScriptPath = os.path.join(kuzuRoot, "scripts", "export-dbs.py")
    execPath = os.path.join(kuzuRoot, "build", "relwithdebinfo", "tools", "shell", "kuzu")
    runCommand(["python3", exportScriptPath, execPath, datasetDir], cwd=kuzuRoot)

    # Determine export path based on version (taken from exportScript)
    version = getVersion(execPath)
    if version is None:
        print("Failed to determine version. Aborting.")
        return 1
    exportPath = os.path.join(datasetDir, "tmp", version)

    # Checkout commit B and run tests
    runCommand(["git", "checkout", commitB], cwd=kuzuRoot)
    env = os.environ.copy()
    env["E2E_IMPORT_DB_DIR"] = exportPath
    runCommand(["make", "test"], cwd=kuzuRoot, env=env)

    # Restore original branch
    runCommand(["git", "checkout", currentBranch], cwd=kuzuRoot)

    return 0


if __name__ == '__main__':
    sys.exit(main())
