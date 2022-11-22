add_executable(syntax_error_test
        syntax_error_test.cpp)

target_link_libraries(syntax_error_test
        PRIVATE
        test_utility)

add_test(kuzu_syntax_error_test syntax_error_test)
