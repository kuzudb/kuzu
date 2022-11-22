add_executable(types_test
        date_test.cpp
        interval_test.cpp
        time_test.cpp
        timestamp_test.cpp
        types_test.cpp)

target_link_libraries(types_test
        PUBLIC
        test_utility)

add_test(kuzu_types_test types_test)
add_subdirectory(vector)
