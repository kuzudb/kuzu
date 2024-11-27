if (NOT TARGET e2e_test)
    add_kuzu_test(e2e_test e2e_test.cpp)
endif()

if (NOT TARGET e2e_test_extension)
    add_kuzu_test(e2e_test_extension e2e_test.cpp)
endif()

if (NOT TARGET cleanup_test)
    add_kuzu_test(cleanup_test cleanup_test.cpp)
endif()

if (NOT TARGET cleanup_test_extension)
    add_kuzu_test(cleanup_test_extension cleanup_test.cpp)
endif()
