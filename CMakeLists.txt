cmake_minimum_required(VERSION 3.11)

project(Kuzu VERSION 0.0.3 LANGUAGES CXX)

find_package(Threads REQUIRED)

set(CMAKE_FIND_PACKAGE_RESOLVE_SYMLINKS TRUE)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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
if(${ENABLE_THREAD_SANITIZER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fno-common -fpermissive")
endif()
if(${ENABLE_ADDRESS_SANITIZER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fno-common -fpermissive")
endif()
if(${ENABLE_UBSAN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-common -fpermissive")
endif()
option(BUILD_TESTS "Build C++ and Python tests." FALSE)
option(BUILD_BENCHMARK "Build benchmarks." FALSE)

option(BUILD_LCOV "Build coverage report." FALSE)
if(${BUILD_LCOV})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

function(link_threads LIBRARY)
    if (CMAKE_VERSION VERSION_LESS "3.1")
        target_link_libraries(${LIBRARY} pthread)
    else ()
        target_link_libraries(${LIBRARY} Threads::Threads)
    endif ()
endfunction()

function(add_kuzu_test TEST_NAME)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PRIVATE test_helper graph_test)
    target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include)
    include(GoogleTest)
    gtest_discover_tests(${TEST_NAME})
endfunction()

add_definitions(-DKUZU_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")
add_definitions(-DKUZU_STORAGE_VERSION="${CMAKE_PROJECT_VERSION}")

set(ARROW_INSTALL ${CMAKE_CURRENT_SOURCE_DIR}/external/build/arrow/install)
find_library(ARROW_DEPS_PATH arrow_bundled_dependencies HINTS ${ARROW_INSTALL}/lib ${ARROW_INSTALL}/lib64)
find_library(PARQUET_PATH parquet HINTS ${ARROW_INSTALL}/lib ${ARROW_INSTALL}/lib64)
find_library(ARROW_PATH arrow HINTS ${ARROW_INSTALL}/lib ${ARROW_INSTALL}/lib64)

add_library(arrow_deps STATIC IMPORTED)
set_target_properties(arrow_deps PROPERTIES IMPORTED_LOCATION ${ARROW_DEPS_PATH})
add_library(parquet_lib STATIC IMPORTED)
set_target_properties(parquet_lib PROPERTIES IMPORTED_LOCATION ${PARQUET_PATH})
add_library(arrow_lib STATIC IMPORTED)
set_target_properties(arrow_lib PROPERTIES IMPORTED_LOCATION ${ARROW_PATH})
include_directories(${ARROW_INSTALL}/include)

include_directories(src/include)
include_directories(third_party/antlr4_cypher/include)
include_directories(third_party/antlr4_runtime/src)
include_directories(third_party/spdlog)
include_directories(third_party/nlohmann_json)
include_directories(third_party/pyparse)
include_directories(third_party/utf8proc/include)
include_directories(third_party/pybind11/include)
include_directories(third_party/re2/include)
include_directories(third_party/concurrentqueue)

add_subdirectory(third_party)
add_subdirectory(src)
if (${BUILD_TESTS})
    add_subdirectory(test)
elseif (${BUILD_BENCHMARK})
    add_subdirectory(test/test_helper)
endif()
add_subdirectory(tools)
