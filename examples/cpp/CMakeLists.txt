add_executable(example-cpp main.cpp)
target_link_libraries(example-cpp kuzu_shared)

add_executable(example-cpp-static main.cpp)
target_link_libraries(example-cpp-static kuzu)
