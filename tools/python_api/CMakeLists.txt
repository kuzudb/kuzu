include(FetchContent)
project(_kuzu)

set(CMAKE_CXX_STANDARD 20)

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

FetchContent_Declare(
        pybind11_sources
        GIT_REPOSITORY https://github.com/pybind/pybind11.git
        GIT_TAG v2.2
)
FetchContent_GetProperties(pybind11_sources)

if(NOT pybind11_sources_POPULATED)
    FetchContent_Populate(pybind11_sources)
    add_subdirectory(
            ${pybind11_sources_SOURCE_DIR}
            ${pybind11_sources_BINARY_DIR}
    )
endif()

pybind11_add_module(_kuzu
        SHARED
        kuzu_binding.cpp
        py_connection.cpp
        py_database.cpp
        py_query_result.cpp
        py_query_result_converter.cpp)

set_target_properties(_kuzu
        PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/")

target_link_libraries(_kuzu
        PRIVATE
        kuzu)

target_include_directories(
        _kuzu
        PUBLIC
        ../../src/include
)
