add_executable(demo_db_test
        demo_db_test.cpp)

target_link_libraries(demo_db_test
        PRIVATE
        test_helper)

add_test(kuzu_demo_db_test demo_db_test)
