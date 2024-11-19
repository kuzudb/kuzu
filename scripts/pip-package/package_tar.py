#!/usr/bin/env python3

import os
import shutil
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _get_kuzu_version():
    cmake_file = os.path.abspath(os.path.join(base_dir, "..", "CMakeLists.txt"))
    with open(cmake_file) as f:
        for line in f:
            if line.startswith("project(Kuzu VERSION"):
                raw_version = line.split(" ")[2].strip()
                version_nums = raw_version.split(".")
                if len(version_nums) <= 3:
                    return raw_version
                else:
                    dev_suffix = version_nums[3]
                    version = ".".join(version_nums[:3])
                    version += ".dev%s" % dev_suffix
                    return version


if __name__ == "__main__":
    if len(sys.argv) == 2:
        file_name = sys.argv[1]
    else:
        file_name = "kuzu-%s.tar.gz" % _get_kuzu_version()
    print("Creating %s..." % file_name)

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

        os.remove(os.path.join(tempdir, "kuzu-source.tar"))

        # Remove components that are not needed for the pip package
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/dataset"))
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/examples"))
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/benchmark"))
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/logo"))
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/extension"))
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/test"))
        shutil.rmtree(os.path.join(tempdir, "kuzu-source/.github"))

        os.makedirs(os.path.join(tempdir, "kuzu"))
        for path in ["setup.py", "setup.cfg", "MANIFEST.in"]:
            shutil.copy2(path, os.path.join(tempdir, path))
        shutil.copy2("../../LICENSE", os.path.join(tempdir, "LICENSE"))
        shutil.copy2("../../README.md", os.path.join(tempdir, "README.md"))

        shutil.copy2(
            "../../tools/python_api/pyproject.toml",
            os.path.join(tempdir, "pyproject.toml"),
        )
        # Update the version in pyproject.toml
        with open(os.path.join(tempdir, "pyproject.toml"), "r") as f:
            lines = f.readlines()
        with open(os.path.join(tempdir, "pyproject.toml"), "w") as f:
            for line in lines:
                if line.startswith("version ="):
                    f.write('version = "%s"\n' % _get_kuzu_version())
                else:
                    f.write(line)
        shutil.copy2("README.md", os.path.join(tempdir, "README_PYTHON_BUILD.md"))
        subprocess.check_call([sys.executable, "setup.py", "egg_info"], cwd=tempdir)
        shutil.copy2(
            os.path.join(tempdir, "kuzu.egg-info", "PKG-INFO"),
            os.path.join(tempdir, "PKG-INFO"),
        )
        with tarfile.open(file_name, "w:gz") as sdist:
            sdist.add(tempdir, "sdist")
