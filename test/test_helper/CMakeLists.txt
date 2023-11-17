add_library(
        test_helper
        OBJECT
        test_helper.cpp
)

target_include_directories(
        test_helper
        PUBLIC
        ../include/
)

target_link_libraries(test_helper PUBLIC kuzu)

add_library(
        api_test_helper
        OBJECT
        test_helper.cpp
)

target_include_directories(
        api_test_helper
        PUBLIC
        ../include/
)

target_link_libraries(api_test_helper PUBLIC kuzu_shared)
