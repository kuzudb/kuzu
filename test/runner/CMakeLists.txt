if (${BUILD_TESTS})
    add_kuzu_test(e2e_test e2e_test.cpp)
endif()

if (${BUILD_EXTENSION_TESTS})
    add_kuzu_extension_test(e2e_test e2e_test.cpp)
endif()

if (${BUILD_TESTS})
    add_kuzu_test(cleanup_test cleanup_test.cpp)
endif()

if (NOT ${BUILD_EXTENSION_TESTS})
    add_kuzu_extension_test(cleanup_test cleanup_test.cpp)
endif()
