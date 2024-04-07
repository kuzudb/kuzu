# `pip install` from the source
```
chmod +x package_tar.py
./package_tar.py
pip install kuzu.tar.gz    
```

Note: installing from source requires the full toolchain for building the project, including Cmake(>=3.11), Python 3, and a compiler compatible with C++20. The package works for both Linux and macOS.

## Bulid wheels for macOS
We use [cibuildwheel](https://github.com/pypa/cibuildwheel). The configuration file is located at `.github/workflows/mac-wheel-workflow.yml`. Although we currently run the pipeline on a self-hosted Mac mini, this configuration also works for GitHub-hosted runners (`macos-11` and `macos-12`).

### Self-hosted macOS runner setup
The self-hosted runner needs to be properly configured for the pipeline to run. Please follow the instructions below to configure a self-hosted runner on macOS.

- OS requirement: for support for C++20 and proper cross-compilation for ARM64, Xcode 13+ and macOS 11+ is required. If the hardware does not support macOS 11 officially, consider using [OpenCore Legacy Patcher](https://dortania.github.io/OpenCore-Legacy-Patcher/).
- Machine configurations:
    - [REQUIRED] Username: The username must be set to `runner` to be consistent with the GitHub-hosted runners. Otherwise, cibuildwheel configuration step will fail due to writing to directories that does not exist.
    - [REQUIRED] `sudo` without password: The `runner` user needs to have the permission to `sudo` without password. Otherwise, cibuildwheel will not be able to install Python automatically due to not able to take user input for the password. To enable `sudo` without password, create a file (with arbitrary name) under `/private/etc/sudoers.d/` and add `runner    ALL = (ALL) NOPASSWD: ALL` to it.
    - [REQUIRED] Keep the machine from going to sleep automatically: under System Preferences > Energy Saver, check "Prevent your Mac from sleeping automatically when the display is off" and uncheck "Put hard disks to sleep when possible". Alternatively, sleep can be disabled with `sudo pmset disablesleep 1`.
    - [OPTIONAL] Automatic login: the GitHub self-hosted runner service on macOS is configured for the user space only. For the listener to be back online automatically after each reboot without manually logging in, automatic login should be turned on for `runner` user. Please follow [this instruction](https://support.apple.com/en-us/HT201476) to configure it.
    - [OPTIONAL] For the ease of remote management, consider enabling `sshd` and configure a DDNS service to keep the hostname updated with the correct IP address.
- Dependencies installation:
    - Xcode toolchain: after installing Xcode, run `xcode-select --install` to install Xcode Command Line Tools.
    - Homebrew: follow the instructions on [brew.sh](https://brew.sh) to install it.
    - CMake: `brew install cmake`.
    - Pipx: `brew install pipx` and `pipx ensurepath`.
- Github self-hosted runners configuration: follow [this documentation](https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners) to add the self-hosted runner and [this documentation](https://docs.github.com/en/actions/hosting-your-own-runners/configuring-the-self-hosted-runner-application-as-a-service) for configuring self-hosted runner as a service.
