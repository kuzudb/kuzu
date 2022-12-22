add_subdirectory(aggregate)
add_subdirectory(copy_csv)
add_subdirectory(ddl)
add_subdirectory(hash_join)
add_subdirectory(intersect)
add_subdirectory(order_by)
add_subdirectory(scan_column)
add_subdirectory(scan_list)
add_subdirectory(table_scan)
add_subdirectory(update)
add_subdirectory(var_length_extend)

add_library(kuzu_processor_operator
        OBJECT
        base_extend.cpp
        base_table_scan.cpp
        cross_product.cpp
        filter.cpp
        filtering_operator.cpp
        flatten.cpp
        generic_extend.cpp
        index_scan.cpp
        limit.cpp
        multiplicity_reducer.cpp
        physical_operator.cpp
        projection.cpp
        result_collector.cpp
        scan_node_id.cpp
        semi_masker.cpp
        shortest_path_adj_list.cpp
        shortest_path_adj_col.cpp
        base_shortest_path.cpp
        skip.cpp
        unwind.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator>
        PARENT_SCOPE)
