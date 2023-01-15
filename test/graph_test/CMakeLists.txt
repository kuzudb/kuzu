add_library(
        graph_test
        OBJECT
        graph_test.cpp)

target_include_directories(
        graph_test
        PUBLIC
        ../include/
        ../../src/include
)

target_link_libraries(graph_test PUBLIC GTest::GTest kuzu)
