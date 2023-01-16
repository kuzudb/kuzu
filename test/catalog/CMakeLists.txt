add_executable(catalog_test catalog_test.cpp)

target_link_libraries(catalog_test PUBLIC graph_test)

add_test(kuzu_catalog_test catalog_test)
