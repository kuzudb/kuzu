cmake_minimum_required(VERSION 3.15)

project(Kuzu VERSION 0.4.2.12 LANGUAGES CXX C)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_FIND_PACKAGE_RESOLVE_SYMLINKS TRUE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
# On Linux, symbols in executables are not accessible by loaded shared libraries (e.g. via dlopen(3)). However, we need to export public symbols in executables so that extensions can access public symbols. This enables that behaviour.
set(CMAKE_ENABLE_EXPORTS TRUE)

option(ENABLE_WERROR "Treat all warnings as errors" FALSE)
if(ENABLE_WERROR)
    if (CMAKE_VERSION VERSION_GREATER "3.24.0" OR CMAKE_VERSION VERSION_EQUAL "3.24.0")
        set(COMPILE_WARNING_AS_ERROR TRUE)
    elseif (MSVC)
        add_compile_options(\WX)
    else ()
        add_compile_options(-Werror)
    endif()
endif()

# Detect OS and architecture, copied from DuckDB
set(OS_NAME "unknown")
set(OS_ARCH "amd64")

string(REGEX MATCH "(arm64|aarch64)" IS_ARM "${CMAKE_SYSTEM_PROCESSOR}")
if(IS_ARM)
  set(OS_ARCH "arm64")
elseif(FORCE_32_BIT)
  set(OS_ARCH "i386")
endif()

if(APPLE)
  set(OS_NAME "osx")
endif()
if(WIN32)
  set(OS_NAME "windows")
endif()
if(UNIX AND NOT APPLE)
  set(OS_NAME "linux") # sorry BSD
endif()

if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(STATUS "64-bit architecture detected")
    add_compile_definitions(__64BIT__)
elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
    message(STATUS "32-bit architecture detected")
    add_compile_definitions(__32BIT__)
    set(__32BIT__ TRUE)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(DEFINED ENV{PYBIND11_PYTHON_VERSION})
    set(PYBIND11_PYTHON_VERSION $ENV{PYBIND11_PYTHON_VERSION})
endif()

if(DEFINED ENV{PYTHON_EXECUTABLE})
    set(PYTHON_EXECUTABLE $ENV{PYTHON_EXECUTABLE})
endif()

find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
else ()
    find_program(CCACHE_PROGRAM sccache)
    if (CCACHE_PROGRAM)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    endif ()
endif ()

set(INSTALL_LIB_DIR
        lib
        CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR
        bin
        CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR
        include
        CACHE PATH "Installation directory for header files")
set(INSTALL_CMAKE_DIR
        ${DEF_INSTALL_CMAKE_DIR}
        CACHE PATH "Installation directory for CMake files")

option(ENABLE_ADDRESS_SANITIZER "Enable address sanitizer." FALSE)
option(ENABLE_THREAD_SANITIZER "Enable thread sanitizer." FALSE)
option(ENABLE_UBSAN "Enable undefined behavior sanitizer." FALSE)
option(ENABLE_RUNTIME_CHECKS "Enable runtime coherency checks (e.g. asserts)" FALSE)
option(ENABLE_LTO "Enable Link-Time Optimization" FALSE)
option(ENABLE_O3_OPTIMIZATION "Enable O3 Optimization" TRUE)
option(ENABLE_IGNORE_WARNINGS "Enable ignore warnings" TRUE)
if(MSVC)
    # Required for M_PI on Windows
    add_compile_definitions(_USE_MATH_DEFINES)
    add_compile_definitions(NOMINMAX)
    add_compile_definitions(SERD_STATIC)
    # TODO (bmwinger): Figure out if this can be set automatically by cmake,
    # or at least better integrated with user-specified options
    # For now, hardcode _AMD64_
    # CMAKE_GENERATOR_PLATFORM can be used for visual studio builds, but not for ninja
    add_compile_definitions(_AMD64_)
    # Non-english windows system may use other encodings other than utf-8 (e.g. Chinese use GBK).
    add_compile_options("/utf-8")
    # Enables support for custom hardware exception handling
    add_compile_options("/EHa")
    # Remove the default to avoid warnings
    STRING(REPLACE "/EHsc" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    STRING(REPLACE "/EHs" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    # Store all libraries and binaries in the same directory so that kuzu_shared.dll is found at runtime
    set(LIBRARY_OUTPUT_PATH "${CMAKE_BINARY_DIR}/src")
    set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/src")

    string(REGEX REPLACE "/W[3|4]" "/w" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    add_compile_options($<$<CONFIG:Release>:/W0>)
else()
    add_compile_options(-Wall -Wextra)
    # Disable warnings for unknown pragmas, which is used by several third-party libraries
    add_compile_options(-Wno-unknown-pragmas)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wno-restrict) # no restrict until https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105651 is fixed
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -g")

if (${ENABLE_O3_OPTIMIZATION})
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif()

if (${ENABLE_IGNORE_WARNINGS})
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -w")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -w")
endif()

if(${ENABLE_THREAD_SANITIZER})
    if(MSVC)
        message(FATAL_ERROR "Thread sanitizer is not supported on MSVC")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fno-omit-frame-pointer")
    endif()
endif()
if(${ENABLE_ADDRESS_SANITIZER})
    if(MSVC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fsanitize=address")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    endif()
endif()
if(${ENABLE_UBSAN})
    if(MSVC)
        message(FATAL_ERROR "Undefined behavior sanitizer is not supported on MSVC")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer")
    endif()
endif()

if(${ENABLE_RUNTIME_CHECKS})
    add_compile_definitions(KUZU_RUNTIME_CHECKS)
endif()

if(${ENABLE_LTO})
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

option(AUTO_UPDATE_GRAMMAR "Automatically regenerate C++ grammar files on change." TRUE)
option(BUILD_BENCHMARK "Build benchmarks." FALSE)
option(BUILD_EXTENSIONS "Semicolon-separated list of extensions to build." "")
option(BUILD_EXAMPLES "Build examples." FALSE)
option(BUILD_JAVA "Build Java API." FALSE)
option(BUILD_NODEJS "Build NodeJS API." FALSE)
option(BUILD_PYTHON "Build Python API." FALSE)
option(BUILD_SHELL "Build Interactive Shell" TRUE)
option(BUILD_BENCHMARK_VECTOR "Build vector benchmarks." TRUE)
option(BUILD_SINGLE_FILE_HEADER "Build single file header. Requires Python >= 3.9." FALSE)
option(BUILD_TESTS "Build C++ tests." FALSE)
option(BUILD_EXTENSION_TESTS "Build C++ extension tests." FALSE)
option(BUILD_KUZU "Build Kuzu." TRUE)
option(ENABLE_BACKTRACES "Enable backtrace printing for exceptions and segfaults" FALSE)

option(BUILD_LCOV "Build coverage report." FALSE)
if(${BUILD_LCOV})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

if (ENABLE_BACKTRACES)
    find_package(cpptrace)
    if (NOT cpptrace_FOUND)
        include(FetchContent)
        FetchContent_Declare(
          cpptrace
          GIT_REPOSITORY https://github.com/jeremy-rifkin/cpptrace.git
          GIT_TAG        v0.5.4
          GIT_SHALLOW    TRUE
        )
        FetchContent_MakeAvailable(cpptrace)
    endif()
    add_compile_definitions(KUZU_BACKTRACE)
endif()

function(add_kuzu_test TEST_NAME)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PRIVATE test_helper test_runner graph_test)
    if (ENABLE_BACKTRACES)
        target_link_libraries(${TEST_NAME} PRIVATE register_backtrace_signal_handler)
    endif()
    target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include)
    include(GoogleTest)
    gtest_discover_tests(${TEST_NAME} DISCOVERY_TIMEOUT 600 DISCOVERY_MODE PRE_TEST)
endfunction()

function(add_kuzu_api_test TEST_NAME)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PRIVATE api_graph_test api_test_helper)
    if (ENABLE_BACKTRACES)
        target_link_libraries(${TEST_NAME} PRIVATE register_backtrace_signal_handler)
    endif()
    target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include)
    include(GoogleTest)
    gtest_discover_tests(${TEST_NAME})
endfunction()

# Windows doesn't support dynamic lookup, so we have to link extensions against kuzu.
if (MSVC AND (NOT BUILD_EXTENSIONS EQUAL ""))
    set(BUILD_KUZU TRUE)
endif ()

include_directories(third_party/antlr4_runtime/src)
include_directories(third_party/antlr4_cypher/include)
include_directories(third_party/brotli/c/include)
include_directories(third_party/fast_float/include)
include_directories(third_party/mbedtls/include)
include_directories(third_party/miniparquet/src)
include_directories(third_party/miniz)
include_directories(third_party/nlohmann_json)
include_directories(third_party/pybind11/include)
include_directories(third_party/pyparse)
include_directories(third_party/re2/include)
include_directories(third_party/serd/include)
if (${BUILD_TESTS})
    include_directories(third_party/spdlog)
elseif (${BUILD_BENCHMARK})
    include_directories(third_party/spdlog)
endif ()
include_directories(third_party/utf8proc/include)
include_directories(third_party/zstd/include)
include_directories(third_party/httplib)
include_directories(third_party/pcg)
include_directories(third_party/simsimd/include)

add_subdirectory(third_party)

if(${BUILD_KUZU})
add_definitions(-DKUZU_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")
add_definitions(-DKUZU_CMAKE_VERSION="${CMAKE_PROJECT_VERSION}")
add_definitions(-DKUZU_EXTENSION_VERSION="0.3.20")

include_directories(src/include)

add_subdirectory(src)
if (${BUILD_TESTS})
    add_definitions(-DTEST_FILES_DIR="test/test_files")
    add_subdirectory(test)
elseif (${BUILD_BENCHMARK})
    add_definitions(-DTEST_FILES_DIR="test/test_files")
    add_subdirectory(test/test_helper)
endif ()
add_subdirectory(tools)
endif ()
if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/extension/CMakeLists.txt")
    add_subdirectory(extension)
endif ()

if (${BUILD_EXAMPLES})
    add_subdirectory(examples/c)
    add_subdirectory(examples/cpp)
endif()
