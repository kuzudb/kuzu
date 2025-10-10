if (${BUILD_EXTENSION_TESTS})
    add_kuzu_test(vector_prepare_test prepare_test.cpp)
    add_kuzu_test(vector_parallel_loading_test parallel_loading_test.cpp)
endif ()
