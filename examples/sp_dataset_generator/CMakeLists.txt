add_executable(example_cpp2 main.cpp)
target_link_libraries(example_cpp2 kuzu_shared)
target_include_directories(example_cpp2 PRIVATE ${CMAKE_BINARY_DIR}/src)
add_dependencies(example_cpp2 single_file_header)
