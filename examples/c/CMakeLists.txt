add_executable(example_c main.c)
target_link_libraries(example_c kuzu_shared)
target_include_directories(example_c PRIVATE ${PROJECT_SOURCE_DIR}/src/include/c_api)
