# CMake generated Testfile for 
# Source directory: /Users/calvin/Desktop/kuzu_cmake/test
# Build directory: /Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/test
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(PythonAPI.DataType "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_datatype.py")
set_tests_properties(PythonAPI.DataType PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;33;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
add_test(PythonAPI.PandaAPI "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_df.py")
set_tests_properties(PythonAPI.PandaAPI PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;34;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
add_test(PythonAPI.Exception "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_exception.py")
set_tests_properties(PythonAPI.Exception PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;35;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
add_test(PythonAPI.GetHeader "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_get_header.py")
set_tests_properties(PythonAPI.GetHeader PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;36;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
add_test(PythonAPI.Main "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_main.py")
set_tests_properties(PythonAPI.Main PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;37;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
add_test(PythonAPI.Parameter "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_parameter.py")
set_tests_properties(PythonAPI.Parameter PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;38;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
add_test(PythonAPI.WriteToCSV "-m" "pytest" "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test/test_write_to_csv.py")
set_tests_properties(PythonAPI.WriteToCSV PROPERTIES  WORKING_DIRECTORY "/Users/calvin/Desktop/kuzu_cmake/tools/python_api/test" _BACKTRACE_TRIPLES "/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;28;add_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;39;add_kuzu_python_api_test;/Users/calvin/Desktop/kuzu_cmake/test/CMakeLists.txt;0;")
subdirs("../_deps/googletest-build")
subdirs("test_helper")
subdirs("binder")
subdirs("catalog")
subdirs("common")
subdirs("copy_csv")
subdirs("demo_db")
subdirs("main")
subdirs("parser")
subdirs("processor")
subdirs("runner")
subdirs("storage")
subdirs("transaction")
