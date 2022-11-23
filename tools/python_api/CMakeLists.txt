include(FetchContent)
project(kuzu_python_api)

set(CMAKE_CXX_STANDARD 20)

find_package(PythonInterp REQUIRED)

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

pybind11_add_module(kuzu_python_api
        kuzu_binding.cpp
        py_connection.cpp
        py_database.cpp
        py_query_result.cpp
        py_query_result_converter.cpp)

target_link_libraries(kuzu_python_api
        PUBLIC
        kuzu
)

target_include_directories(
        kuzu_python_api
        PUBLIC
        ../../src/include
)

add_subdirectory(test)

