# SimSIMD library CMakeLists.txt
cmake_minimum_required(VERSION 3.14 FATAL_ERROR)
project(
        simsimd
        VERSION 6.2.3
        LANGUAGES C CXX
        DESCRIPTION "Portable mixed-precision BLAS-like vector math library for x86 and ARM"
        HOMEPAGE_URL "https://github.com/ashvardanian/simsimd"
)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED YES)
set(CMAKE_C_EXTENSIONS NO)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS NO)

# Determine if SimSIMD is built as a subproject (using `add_subdirectory`) or if it is the main project
set(SIMSIMD_IS_MAIN_PROJECT OFF)

if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(SIMSIMD_IS_MAIN_PROJECT ON)
endif ()

option(SIMSIMD_BUILD_SHARED "Compile a dynamic library" ${SIMSIMD_IS_MAIN_PROJECT})
option(SIMSIMD_BUILD_TESTS "Small compilation tests compile-time and run-time dispatch" OFF)
option(SIMSIMD_BUILD_BENCHMARKS "Compile micro-benchmarks for current ISA" OFF)
option(SIMSIMD_BUILD_BENCHMARKS_WITH_CBLAS "Include BLAS in micro-kernel benchmarks" OFF)

# Default to Release build type if not set
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

# Global compiler flags for debug and release
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_C_FLAGS_DEBUG "-g -fsanitize=address")
set(CMAKE_C_FLAGS_RELEASE "-O3")

# Compiler-specific flags
if (CMAKE_CXX_COMPILER_ID MATCHES "^(Apple)?Clang$")
    if (NOT APPLE)
        add_compile_options(-march=native)
    endif ()

    add_compile_options(-pedantic -ferror-limit=1)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-march=native -pedantic -fmax-errors=1 -Wno-tautological-constant-compare)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "IntelLLVM")
    add_compile_options(-w -ferror-limit=1)
endif ()

# Define the header-only library
file(GLOB SIMSIMD_SOURCES include/simsimd/*.h)
add_library(simsimd INTERFACE)
target_sources(simsimd INTERFACE ${SIMSIMD_SOURCES})
target_include_directories(simsimd INTERFACE "${PROJECT_SOURCE_DIR}/include")

# Build benchmarks if required
if (SIMSIMD_BUILD_BENCHMARKS)
    # Fetch external dependencies
    include(FetchContent)

    # Suppress building tests of Google Benchmark
    set(BENCHMARK_ENABLE_TESTING OFF)
    set(BENCHMARK_ENABLE_INSTALL OFF)
    set(BENCHMARK_ENABLE_DOXYGEN OFF)
    set(BENCHMARK_INSTALL_DOCS OFF)
    set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF)
    set(BENCHMARK_USE_BUNDLED_GTEST ON)

    FetchContent_Declare(
            benchmark
            GIT_REPOSITORY https://github.com/google/benchmark.git
            GIT_TAG v1.7.0
    )
    FetchContent_MakeAvailable(benchmark)

    # Remove the Google Benchmark's "built in debug warning"
    if (CMAKE_BUILD_TYPE STREQUAL "Release")
        target_compile_definitions(benchmark PRIVATE NDEBUG)
    endif ()

    find_package(Threads REQUIRED)
    add_executable(simsimd_bench scripts/bench.cxx)
    target_link_libraries(simsimd_bench simsimd Threads::Threads benchmark)

    if (SIMSIMD_BUILD_BENCHMARKS_WITH_CBLAS)
        find_package(BLAS REQUIRED)
        if (BLAS_FOUND)
            message(STATUS "BLAS found: ${BLAS_LIBRARIES}")
            include_directories(${BLAS_INCLUDE_DIRS})
            target_include_directories(simsimd_bench PRIVATE ${BLAS_INCLUDE_DIRS})
            target_link_libraries(simsimd_bench ${BLAS_LIBRARIES})
            target_compile_definitions(simsimd_bench PRIVATE SIMSIMD_BUILD_BENCHMARKS_WITH_CBLAS=1)
        else ()
            message(FATAL_ERROR "BLAS not found")
        endif ()
    endif ()

endif ()

if (SIMSIMD_BUILD_TESTS)
    add_executable(simsimd_test_compile_time scripts/test.c)
    target_link_libraries(simsimd_test_compile_time simsimd m)

    add_executable(simsimd_test_run_time scripts/test.c c/lib.c)
    target_compile_definitions(simsimd_test_run_time PRIVATE SIMSIMD_DYNAMIC_DISPATCH=1)
    target_link_libraries(simsimd_test_run_time simsimd m)
endif ()

if (SIMSIMD_BUILD_SHARED)
    set(SIMSIMD_SOURCES ${SIMSIMD_SOURCES} c/lib.c)
    add_library(simsimd_shared SHARED ${SIMSIMD_SOURCES})
    target_include_directories(simsimd_shared PUBLIC "${PROJECT_SOURCE_DIR}/include")
    set_target_properties(simsimd_shared PROPERTIES OUTPUT_NAME simsimd)

    install(
            TARGETS simsimd_shared
            ARCHIVE
            BUNDLE
            FRAMEWORK
            LIBRARY
            OBJECTS
            PRIVATE_HEADER
            PUBLIC_HEADER
            RESOURCE
            RUNTIME)
endif ()

install(DIRECTORY ./include/ DESTINATION /usr/include/)
install(DIRECTORY ./c/ DESTINATION /usr/src/${PROJECT_NAME}/)
