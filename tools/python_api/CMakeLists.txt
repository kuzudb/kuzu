file(GLOB SOURCE_PY
        "src_py/*")

pybind11_add_module(_kuzu
        SHARED
        src_cpp/kuzu_binding.cpp
        src_cpp/py_connection.cpp
        src_cpp/py_database.cpp
        src_cpp/py_prepared_statement.cpp
        src_cpp/py_query_result.cpp
        src_cpp/py_query_result_converter.cpp
        src_cpp/pandas/pandas_bind.cpp
        src_cpp/pandas/pandas_scan.cpp
        src_cpp/numpy/numpy_type.cpp
        src_cpp/numpy/numpy_scan.cpp)

target_link_libraries(_kuzu
        PRIVATE
        kuzu)

target_include_directories(
        _kuzu
        PUBLIC
        src_cpp/include)

set(PYTHON_INSTALL_DEST ${CMAKE_INSTALL_LIBDIR}/python/kuzu)
install(FILES ${SOURCE_PY} DESTINATION ${PYTHON_INSTALL_DEST})
install(TARGETS _kuzu DESTINATION ${PYTHON_INSTALL_DEST})
