import shutil
import subprocess
import os
import sys

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py as _build_py

base_dir = os.path.dirname(__file__)

with open(os.path.join(base_dir, 'tools', 'python_api', 'requirements_dev.txt')) as f:
    requirements = f.read().splitlines()


class BazelExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class BazelBuild(build_ext):
    def build_extension(self, ext: BazelExtension) -> None:
        self.announce("Building native extension...", level=3)
        args = ['--cxxopt=-std=c++2a', '--cxxopt=-O3']
        # It seems bazel does not automatically pick up MACOSX_DEPLOYMENT_TARGET
        # from the environment, so we need to pass it explicitly.
        if "MACOSX_DEPLOYMENT_TARGET" in os.environ:
            args.append("--macos_minimum_os=" +
                        os.environ["MACOSX_DEPLOYMENT_TARGET"])
        full_cmd = ['bazel', 'build', *args, '//tools/python_api:all']
        env_vars = os.environ.copy()
        env_vars['PYTHON_BIN_PATH'] = sys.executable
        subprocess.run(full_cmd, cwd=ext.sourcedir, check=True, env=env_vars)
        self.announce("Done building native extension", level=3)
        self.announce("Copying native extension...", level=3)
        shutil.copyfile(os.path.join(ext.sourcedir, 'bazel-bin', 'tools', 'python_api',
                                     '_graphflowdb.so'), os.path.join(ext.sourcedir, ext.name, '_graphflowdb.so'))
        self.announce("Done copying native extension", level=3)
        # Remove bazel's BUILD file for macOS due to naming conflict with python's build file
        if sys.platform == 'darwin':
            os.remove(os.path.join(ext.sourcedir, 'BUILD'))


class BuildExtFirst(_build_py):
    # Override the build_py command to build the extension first
    def run(self):
        self.run_command("build_ext")
        return super().run()


setup(name='graphflowdb',
      version=os.environ['PYTHON_PACKAGE_VERSION'] if 'PYTHON_PACKAGE_VERSION' in os.environ else '0.0.1',
      install_requires=requirements,
      ext_modules=[BazelExtension(
          name="graphflowdb", sourcedir=base_dir)],
      description='GraphflowDB Python API',
      long_description=open(os.path.join(base_dir, "README.md"), 'r').read(),
      long_description_content_type="text/markdown",
      packages=["graphflowdb"],
      zip_safe=True,
      include_package_data=True,
      cmdclass={
          'build_py': BuildExtFirst,
          'build_ext': BazelBuild,
      }
      )
