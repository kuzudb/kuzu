import shutil
import subprocess
import os
import sys

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py as _build_py

base_dir = os.path.dirname(__file__)
print("base_dir: " + base_dir)

with open(os.path.join(base_dir, 'kuzu-source', 'tools', 'python_api', 'requirements_dev.txt')) as f:
    requirements = f.read().splitlines()


class BuildExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)
        print("sourcedir: " + self.sourcedir)


class Build(build_ext):
    def build_extension(self, ext: BuildExtension) -> None:
        self.announce("Building native extension...", level=3)
        full_cmd = ['make', 'release', 'NUM_THREADS=10']
        env_vars = os.environ.copy()
        env_vars['PYTHON_BIN_PATH'] = sys.executable
        build_dir = os.path.join(ext.sourcedir, 'kuzu-source')

        subprocess.run(full_cmd, cwd=build_dir, check=True, env=env_vars)
        self.announce("Done building native extension.", level=3)
        self.announce("Copying native extension...", level=3)
        shutil.copyfile(os.path.join(build_dir, 'tools', 'python_api', 'build', '_kuzu.so'),
                        os.path.join(ext.sourcedir, ext.name, '_kuzu.so'))
        self.announce("Done copying native extension.", level=3)


class BuildExtFirst(_build_py):
    # Override the build_py command to build the extension first.
    def run(self):
        self.run_command("build_ext")
        return super().run()


setup(name='kuzu',
      version=os.environ['PYTHON_PACKAGE_VERSION'] if 'PYTHON_PACKAGE_VERSION' in os.environ else '0.0.2',
      install_requires=requirements,
      ext_modules=[BuildExtension(
          name="kuzu", sourcedir=base_dir)],
      description='KuzuDB Python API',
      license='MIT',
      long_description=open(os.path.join(base_dir, "README.md"), 'r').read(),
      long_description_content_type="text/markdown",
      packages=["kuzu"],
      zip_safe=True,
      include_package_data=True,
      cmdclass={
          'build_py': BuildExtFirst,
          'build_ext': Build,
      }
      )
