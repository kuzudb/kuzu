add_kuzu_test(npy_reader_test npy_reader_test.cpp)
if (${ENABLE_REMOTE_FS})
        add_kuzu_test(copy_gcs_test copy_gcs_test.cpp)
endif()
