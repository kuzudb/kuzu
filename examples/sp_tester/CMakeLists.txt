add_executable(sp_tester main.cpp)
target_link_libraries(sp_tester kuzu_shared)
target_include_directories(sp_tester PRIVATE ${CMAKE_BINARY_DIR}/src)
add_dependencies(sp_tester single_file_header)
