include(FetchContent)
project(_kuzu)

set(CMAKE_CXX_STANDARD 20)

file(GLOB SOURCE_PY
        "src_py/*")

pybind11_add_module(_kuzu
        SHARED
        src_cpp/kuzu_binding.cpp
        src_cpp/cached_import/py_cached_item.cpp
        src_cpp/cached_import/py_cached_import.cpp
        src_cpp/py_connection.cpp
        src_cpp/py_database.cpp
        src_cpp/py_prepared_statement.cpp
        src_cpp/py_query_result.cpp
        src_cpp/py_query_result_converter.cpp
        src_cpp/py_scan_config.cpp
        src_cpp/py_udf.cpp
        src_cpp/py_conversion.cpp
        src_cpp/pyarrow/pyarrow_bind.cpp
        src_cpp/pyarrow/pyarrow_scan.cpp
        src_cpp/pandas/pandas_bind.cpp
        src_cpp/pandas/pandas_scan.cpp
        src_cpp/pandas/pandas_analyzer.cpp
        src_cpp/numpy/numpy_type.cpp
        src_cpp/numpy/numpy_scan.cpp)

set_target_properties(_kuzu
        PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/kuzu"
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/kuzu"
        ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build/kuzu")

target_link_libraries(_kuzu
        PRIVATE
        kuzu)

target_include_directories(
        _kuzu
        PUBLIC
        src_cpp/include)

get_target_property(PYTHON_DEST _kuzu LIBRARY_OUTPUT_DIRECTORY)

file(COPY ${SOURCE_PY} DESTINATION ${PYTHON_DEST})
