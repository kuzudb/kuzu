add_executable(example_cpp main.cpp)
target_link_libraries(example_cpp kuzu_shared)
target_include_directories(example_cpp PRIVATE ${CMAKE_BINARY_DIR}/src)
add_dependencies(example_cpp single_file_header)
