import shutil
import subprocess
import multiprocessing
import os
import sys

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py as _build_py

num_cores = multiprocessing.cpu_count()
base_dir = os.path.dirname(__file__)

with open(os.path.join(base_dir, 'kuzu-source', 'tools', 'python_api', 'requirements_dev.txt')) as f:
    requirements = f.read().splitlines()


def _get_kuzu_version():
    cmake_file = os.path.join(base_dir, 'kuzu-source', 'CMakeLists.txt')
    with open(cmake_file) as f:
        for line in f:
            if line.startswith('project(Kuzu VERSION'):
                raw_version = line.split(' ')[2].strip()
                version_nums = raw_version.split('.')
                if len(version_nums) <= 3:
                    return raw_version
                else:
                    dev_suffix = version_nums[3]
                    version = '.'.join(version_nums[:3])
                    version += ".dev%s" % dev_suffix
                    return version

kuzu_version = _get_kuzu_version()
print("The version of this build is %s" % kuzu_version)


class CMakeExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def build_extension(self, ext: CMakeExtension) -> None:
        self.announce("Building native extension...")
        # Pass the platform architecture for arm64 to cmake for
        # cross-compilation.
        env_vars = os.environ.copy()
        python_version = '.'.join(
            (str(sys.version_info.major),  str(sys.version_info.minor)))
        self.announce("Python version is %s" % python_version)
        env_vars['PYBIND11_PYTHON_VERSION'] = python_version
        env_vars['PYTHON_EXECUTABLE'] = sys.executable

        if sys.platform == 'darwin':
            archflags = os.getenv("ARCHFLAGS", "")

            if len(archflags) > 0:
                self.announce("The ARCHFLAGS is set to '%s'." %
                              archflags)
                if archflags == "-arch arm64":
                    env_vars['CMAKE_OSX_ARCHITECTURES'] = "arm64"
                elif archflags == "-arch x86_64":
                    env_vars['CMAKE_OSX_ARCHITECTURES'] = "x86_64"
                else:
                    self.announce(
                        "The ARCHFLAGS is not valid and will be ignored.")
            else:
                self.announce("The ARCHFLAGS is not set.")

            deploy_target = os.getenv("MACOSX_DEPLOYMENT_TARGET", "")
            if len(deploy_target) > 0:
                self.announce("The deployment target is set to '%s'." %
                              deploy_target)
                env_vars['CMAKE_OSX_DEPLOYMENT_TARGET'] = deploy_target

        build_dir = os.path.join(ext.sourcedir, 'kuzu-source')

        # Clean the build directory.
        subprocess.run(['make', 'clean'], check=True, cwd=build_dir)

        # Build the native extension.
        full_cmd = ['make', 'python', 'NUM_THREADS=%d' % num_cores]
        subprocess.run(full_cmd, cwd=build_dir, check=True, env=env_vars)
        self.announce("Done building native extension.")
        self.announce("Copying native extension...")
        dst = os.path.join(ext.sourcedir, ext.name)
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(os.path.join(build_dir, 'tools', 'python_api', 'build',
                                     ext.name), dst)
        self.announce("Done copying native extension.")


class BuildExtFirst(_build_py):
    # Override the build_py command to build the extension first.
    def run(self):
        self.run_command("build_ext")
        return super().run()


setup(name='kuzu',
      version=kuzu_version,
      install_requires=[],
      ext_modules=[CMakeExtension(
          name="kuzu", sourcedir=base_dir)],
      description='An in-process property graph database management system built for query speed and scalability.',
      license='MIT',
      long_description=open(os.path.join(base_dir, "README.md"), 'r').read(),
      long_description_content_type="text/markdown",
      packages=["kuzu"],
      zip_safe=True,
      include_package_data=True,
      cmdclass={
          'build_py': BuildExtFirst,
          'build_ext': CMakeBuild,
      }
      )
