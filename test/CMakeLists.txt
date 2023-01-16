include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0
)
FetchContent_MakeAvailable(googletest)
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)
target_link_libraries(GTest::GTest INTERFACE gmock_main)

enable_testing()
add_subdirectory(test_helper)
add_subdirectory(graph_test)
add_subdirectory(binder)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(copy)
add_subdirectory(demo_db)
add_subdirectory(main)
add_subdirectory(parser)
add_subdirectory(processor)
add_subdirectory(runner)
add_subdirectory(storage)
add_subdirectory(transaction)

function(add_kuzu_python_api_test TEST_NAME FILE_NAME)
    add_test(NAME PythonAPI.${TEST_NAME}
            COMMAND ${PYTHON_EXECUTABLE} -m pytest ${PROJECT_SOURCE_DIR}/tools/python_api/test/${FILE_NAME}
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tools/python_api/test)
endfunction()

add_kuzu_python_api_test(DataType test_datatype.py)
add_kuzu_python_api_test(PandaAPI test_df.py)
add_kuzu_python_api_test(Exception test_exception.py)
add_kuzu_python_api_test(GetHeader test_get_header.py)
add_kuzu_python_api_test(Main test_main.py)
add_kuzu_python_api_test(Parameter test_parameter.py)
add_kuzu_python_api_test(WriteToCSV test_write_to_csv.py)
