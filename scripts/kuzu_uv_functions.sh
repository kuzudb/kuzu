# Kuzu uv Python integration functions
# Add this to your ~/.zshrc: source /path/to/kuzu/scripts/kuzu_uv_functions.sh

# Auto-detect and setup uv Python for Kuzu builds
kuzu_uv_setup() {
    local uv_python_path=$(uv run which python 2>/dev/null)
    
    if [ -z "$uv_python_path" ]; then
        echo "Error: Could not find uv Python installation"
        echo "Make sure you have uv installed and a Python environment active"
        return 1
    fi
    
    local uv_python_root=$(dirname "$(dirname "$uv_python_path")")
    
    export PYTHON_EXECUTABLE="$uv_python_path"
    export Python_ROOT_DIR="$uv_python_root"
    
    echo "uv Python environment set up:"
    echo "  PYTHON_EXECUTABLE=$uv_python_path"
    echo "  Python_ROOT_DIR=$uv_python_root"
}

# Configure cmake with uv Python
kuzu_cmake_uv() {
    local build_dir="${1:-build}"
    local extra_flags="${@:2}"
    
    # Auto-setup if not already done
    if [ -z "$PYTHON_EXECUTABLE" ]; then
        kuzu_uv_setup
    fi
    
    cmake -B "$build_dir" -S . \
        -DBUILD_PYTHON=ON \
        -DPYBIND11_FINDPYTHON=ON \
        -DPython_ROOT_DIR="$Python_ROOT_DIR" \
        $extra_flags
    
    echo "CMake configuration complete. Build directory: $build_dir"
}

# Build with uv Python
kuzu_build_uv() {
    local build_dir="${1:-build}"
    local num_threads="${NUM_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)}"
    kuzu_cmake_uv "$build_dir"
    echo "Building with uv Python using $num_threads threads..."
    make -C "$build_dir" -j "$num_threads"
}

# Quick Python API build with uv
kuzu_python_uv() {
    local build_dir="${1:-build}"
    local num_threads="${NUM_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)}"
    kuzu_cmake_uv "$build_dir" -DBUILD_SHELL=FALSE
    echo "Building Python API with uv Python using $num_threads threads..."
    make -C "$build_dir" _kuzu -j "$num_threads"
} 