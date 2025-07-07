import argparse
import os
import sys
import subprocess


# Get version number like 0.10.0.5.
def getVersion(executablePath):
    try:
        result = subprocess.run(
            [executablePath, "--version"],
            capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        if output.startswith("Kuzu "):
            return output.split(" ", 1)[1]
        else:
            print("Unexpected version format.")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error running executable: {e}")
        return None


# Parse schema.cypher and copy.cypher files.
def createCypherQueries(filePath):
    commands = []
    try:
        with open(filePath, "r") as f:
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
def findValidDatasetDirs(datasetRoot):
    validDirs = []

    for root, dirs, files in os.walk(datasetRoot):
        # This script creates a tmp directory with the exported dbs, we should
        # skip it in our search.
        if "tmp" in root.split(os.sep):
            continue
        fileSet = set(files)

        if "schema.cypher" in fileSet:
            validDirs.append(root)

    return validDirs


# Example scripts/export-dbs.py build/debug/tools/shell/kuzu dataset.
def main():
    parser = argparse.ArgumentParser(description="""Export DBS with
    KUZU shell and dataset paths""")

    parser.add_argument("executablePath", help="Path to the KUZU shell")
    parser.add_argument("datasetPath", help="Path to the dataset directory")
    parser.add_argument("outputDir", help="Path to export the datasets")
    args = parser.parse_args()

    argExecutablePath = os.path.abspath(args.executablePath)
    argDatasetPath = os.path.abspath(args.datasetPath)
    outputDir = os.path.abspath(args.outputDir)

    if not os.path.isfile(argExecutablePath):
        print(f"Error: Executable not found at {argExecutablePath}")
        return 1
    if not os.path.exists(argDatasetPath):
        print(f"Error: Dataset path not found at {argDatasetPath}")
        return 1

    version = getVersion(argExecutablePath)
    if not version:
        print(f"Could not pull version number from {argExecutablePath}")
        return 1

    validDatasets = findValidDatasetDirs(argDatasetPath)
    # This is done to construct a full path to replace the relative paths found
    # in copy.cypher files.
    scriptDir = os.path.dirname(os.path.realpath(__file__))
    rootDir = os.path.abspath(os.path.join(scriptDir, ".."))
    for datasetPath in validDatasets:
        schemaCommands = createCypherQueries(os.path.join(datasetPath, "schema.cypher"))
        copyCommands = createCypherQueries(os.path.join(datasetPath, "copy.cypher"))
        combinedCommands = schemaCommands + copyCommands
        datasetName = os.path.relpath(datasetPath, argDatasetPath)
        exportPath = os.path.join(outputDir, version, datasetName)
        exportCommand = f"EXPORT DATABASE '{exportPath}' (format=\"csv\", header=true);"
        combinedCommands.append(exportCommand)
        combinedCommands.insert(0, "CALL threads=1;")
        print(f"Exporting {datasetPath} to {exportPath}")

        process = subprocess.Popen(
            [argExecutablePath],
            stdin=subprocess.PIPE,
            text=True,
            cwd=rootDir
        )

        for cmd in combinedCommands:
            process.stdin.write(cmd.strip() + "\n")
        process.stdin.close()
        process.wait()
    return 0


if __name__ == '__main__':
    sys.exit(main())
