add_library(join_order_pick STATIC
        jo_connection.cpp)

target_include_directories(join_order_pick
        PUBLIC
        ./include
        ../../src/include
        ../../test/include/test_helper)

target_link_libraries(join_order_pick PUBLIC kuzu)

