#!/usr/bin/env python3

import os
import shutil
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory

if __name__ == "__main__":
    with TemporaryDirectory() as tempdir:
        subprocess.check_call(
            [
                "git",
                "archive",
                "--format",
                "tar",
                "-o",
                os.path.join(tempdir, "kuzu-source.tar"),
                "HEAD",
            ],
            cwd="../..",
        )

        with tarfile.open(os.path.join(tempdir, "kuzu-source.tar")) as tar:
            tar.extractall(path=os.path.join(tempdir, "kuzu-source"))

        os.makedirs(os.path.join(tempdir, "kuzu"))
        for path in ["setup.py", "setup.cfg", "MANIFEST.in"]:
            shutil.copy2(path, os.path.join(tempdir, path))
        shutil.copy2("../../LICENSE", os.path.join(tempdir, "LICENSE.txt"))
        shutil.copy2("../../README.md", os.path.join(tempdir, "README.md"))
        shutil.copy2("README.md", os.path.join(tempdir, "README_PYTHON_BUILD.md"))

        subprocess.check_call([sys.executable, "setup.py", "egg_info"], cwd=tempdir)

        with tarfile.open("kuzu.tar.gz", "w:gz") as sdist:
            sdist.add(tempdir, "kuzu")
