import argparse
import os
import sys
import subprocess
import re


def getVersion(executablePath):
    try:
        result = subprocess.run(
            [executablePath, "--version"],
            capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        match = re.search(r"\b(\d+\.\d+\.\d+\.\d+)\b", output)
        if match:
            return match.group(1)
        else:
            print("Version number not found in output.")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error running executable: {e}")
        return None


def normalizeCypherCommands(filePath):
    commands = []
    with open(filePath, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            if not stripped.endswith(";"):
                stripped += ";"
            commands.append(stripped)
    return commands


def findValidDatasetDirs(datasetRoot):
    validDirs = []

    for root, dirs, files in os.walk(datasetRoot):
        fileSet = set(f.lower() for f in files)

        if "schema.cypher" in fileSet and "copy.cypher" in fileSet:
            validDirs.append(root)

    return validDirs


parser = argparse.ArgumentParser(description="""Export DB with executable and
dataset paths""")

parser.add_argument("executablePath", help="Path to the executable file")
parser.add_argument("datasetPath", help="Path to the dataset")
args = parser.parse_args()

executablePath = args.executablePath
datasetPath = args.datasetPath

if not os.path.isfile(executablePath):
    print(f"Error: Executable not found at {executablePath}")
    sys.exit(1)
if not os.path.exists(datasetPath):
    print(f"Error: Dataset path not found at {datasetPath}")
    sys.exit(1)

version = getVersion(executablePath)
if not version:
    print(f"Could not pull version number from {executablePath}")
    sys.exit(1)

validDatasets = findValidDatasetDirs(datasetPath)
for datasetPath in validDatasets:
    schemaCommands = normalizeCypherCommands(os.path.join(datasetPath,
                                                          "schema.cypher"))
    copyCommands = normalizeCypherCommands(os.path.join(datasetPath,
                                                        "copy.cypher"))
    combinedCommands = schemaCommands + copyCommands

    datasetName = os.path.basename(datasetPath.rstrip("/"))
    exportPath = os.path.join(args.datasetPath, "tmp", version, datasetName)
    exportCommand = f"EXPORT DATABASE '{exportPath}' (format=\"csv\", header=true);"
    combinedCommands.append(exportCommand)
    print(f"Exporting {datasetPath} to {exportPath}")

    process = subprocess.Popen(
        [executablePath],
        stdin=subprocess.PIPE,
        text=True
    )

    for cmd in combinedCommands:
        process.stdin.write(cmd.strip() + "\n")
    process.stdin.close()
    process.wait()
