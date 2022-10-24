# `pip install` from the Source
```
chmod +x package_tar.sh
./package_tar.sh
pip install graphflowdb.tar.gz    
```

Note: installing from source requires the full toolchain for building the project, including bazel, OpenJDK, and a compiler compatible with C++20. The package works for both Linux and macOS.

# Container for self-hosted manylinux builder
## Introduction
The container for manylinux builder automatically builds and upload wheels compatiable with `manylinux2014_x86_64` platform tag when it is manually triggered from CI. The spec for manylinux can be found at [https://github.com/pypa/manylinux](https://github.com/pypa/manylinux).

## Build
```
docker build -t graphflow-self-hosted-linux-builder .
```

## Start Container
```
docker run  --name self-hosted-linux-builder --detach --restart=always\
            -e GITHUB_ACCESS_TOKEN=YOUR_GITHUB_ACCESS_TOKEN\
            -e MACHINE_NAME=NAME_OF_THE_PHYSICAL_MACHINE graphflow-self-hosted-linux-builder
```

Note: `GITHUB_ACCESS_TOKEN` is the account-level access token that can be acquired at [GitHub developer settings](https://github.com/settings/tokens).

## Bulid wheels for macOS
We use [cibuildwheel](https://github.com/pypa/cibuildwheel). The configuration file is located at `.github/workflows/mac-wheel-workflow.yml`.