#if (NOT TARGET e2e_test)
    add_kuzu_test(e2e_test e2e_test.cpp)
#endif()

#if (NOT TARGET cleanup_test)
    add_kuzu_test(cleanup_test cleanup_test.cpp)
#endif()
