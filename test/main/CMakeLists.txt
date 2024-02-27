if(MSVC)
    add_kuzu_api_test(main_test
            arrow_test.cpp
            connection_test.cpp
            prepare_test.cpp
            result_value_test.cpp
            storage_driver_test.cpp
            udf_test.cpp)
else()
    add_kuzu_api_test(main_test
            system_config_test.cpp
            arrow_test.cpp
            connection_test.cpp
            db_locking_test.cpp
            prepare_test.cpp
            result_value_test.cpp
            storage_driver_test.cpp
            udf_test.cpp)
endif()

# Also tested for coverage in connection_test.cpp
# but full testing requires some private APIs
add_kuzu_test(transaction_test transaction_test.cpp)
