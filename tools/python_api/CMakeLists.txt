project(_kuzu)

set(CMAKE_CXX_STANDARD 20)

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
