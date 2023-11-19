add_library(
        graph_test
        OBJECT
        base_graph_test.cpp
        graph_test.cpp)

target_include_directories(
        graph_test
        PUBLIC
        ../include/
)

target_link_libraries(graph_test PUBLIC GTest::GTest kuzu)


add_library(
        api_graph_test
        OBJECT
        base_graph_test.cpp)

target_include_directories(
        api_graph_test
        PUBLIC
        ../include/
)

target_link_libraries(api_graph_test PUBLIC GTest::GTest kuzu_shared)
