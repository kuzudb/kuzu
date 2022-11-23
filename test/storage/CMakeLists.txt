add_executable(storage_test
        buffer_manager_test.cpp
        node_insertion_deletion_test.cpp
        rel_insertion_test.cpp
        wal_record_test.cpp
        wal_replayer_test.cpp
        wal_test.cpp)

target_link_libraries(storage_test
        PRIVATE
        test_helper)

add_test(kuzu_storage_test storage_test)
