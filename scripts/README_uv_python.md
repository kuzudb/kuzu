# Kuzu uv Python Integration

This directory contains scripts for users who wish to use `uv`-managed Python installations for building Kuzu with Python bindings.

## Quick Start

Choose your preferred approach:

1. **Simple script**: `./scripts/uv-make build`
2. **Shell functions**: Use `source scripts/kuzu_uv_functions.sh` and then `kuzu_build_uv <>`
3. **Manual**: Set environment variables and run cmake directly

## Problem

By default, pybind11 looks for Python in system locations or pyenv shims, which may not work correctly with `uv`-managed Python installations. This integration automatically detects your current `uv` Python installation and configures cmake accordingly.

## Solutions

### 1. uv-make script (recommended for most users)

```bash
# Configure and build everything with uv Python
./scripts/uv-make build

# Build only Python API
./scripts/uv-make python

# Build and run tests
./scripts/uv-make pytest

# Configure only (no build)
./scripts/uv-make configure

# Clean build directory
./scripts/uv-make clean

# Show help
./scripts/uv-make help

# Parallel builds (much faster!)
NUM_THREADS=20 ./scripts/uv-make build
NUM_THREADS=8 ./scripts/uv-make python
```

### 2. Shell functions (recommended for regular development)

Add to your `~/.zshrc`:
```bash
source /path/to/kuzu/scripts/kuzu_uv_functions.sh
```

Then use the functions:
```bash
# Setup uv Python environment
kuzu_uv_setup

# Configure cmake with uv Python
kuzu_cmake_uv build

# Build with uv Python
kuzu_build_uv build

# Quick Python API build
kuzu_python_uv build
```

### 3. Manual environment variables

```bash
# Get your uv Python path
UV_PYTHON_PATH=$(uv run which python)
UV_PYTHON_ROOT=$(dirname "$(dirname "$UV_PYTHON_PATH")")

# Set environment variables
export PYTHON_EXECUTABLE="$UV_PYTHON_PATH"
export Python_ROOT_DIR="$UV_PYTHON_ROOT"

# Run cmake
cmake -B build -S . \
    -DBUILD_PYTHON=ON \
    -DPYBIND11_FINDPYTHON=ON \
    -DPython_ROOT_DIR="$UV_PYTHON_ROOT"
```

## How it works

1. **Auto-detection**: Scripts use `uv run which python` to find the current uv Python installation
2. **Path extraction**: Extracts the Python root directory from the executable path
3. **Environment setup**: Sets `PYTHON_EXECUTABLE` and `Python_ROOT_DIR` environment variables
4. **Modern pybind11**: Uses `-DPYBIND11_FINDPYTHON=ON` to enable the new FindPython mode
5. **CMake configuration**: Passes the correct paths to cmake

## Files

- `scripts/uv-make` - Self-contained build script with all common targets
- `scripts/kuzu_uv_functions.sh` - Shell functions for integration into your shell profile

## Summary

The solution has been consolidated into these two files:

1. `scripts/uv-make` - A comprehensive, self-contained script that handles:
   - Auto-detection of uv Python installation
   - CMake configuration with correct paths
   - Building all targets or just Python API
   - Running tests
   - Cleaning build directories

2. `scripts/kuzu_uv_functions.sh` - Shell functions for users who prefer to integrate into their shell profile for persistent availability.

Both approaches automatically detect your current uv Python installation and work with any Python version managed by uv, making them future-proof and requiring no manual updates when you upgrade Python versions.

## Parallel Builds

To speed up builds significantly, you can specify the number of parallel threads:

```bash
# Use 20 threads for faster builds
NUM_THREADS=20 ./scripts/uv-make build

# Use 8 threads for Python API build
NUM_THREADS=8 ./scripts/uv-make python

# With shell functions
NUM_THREADS=16 kuzu_build_uv build
```

If `NUM_THREADS` is not set, the scripts will auto-detect the number of CPU cores available on your system.

## Troubleshooting

### "Could not find uv Python installation"
- Make sure `uv` is installed and in your PATH
- Ensure you have a Python environment active: `uv python install 3.13`

### "Python config failure"
- The uv Python installation might be corrupted
- Try: `uv python install 3.13 --force`

### Build errors
- Make sure you're using the same Python version for building and running
- Check that all required Python packages are installed in your uv environment 