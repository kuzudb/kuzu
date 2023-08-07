add_subdirectory(aggregate)
add_subdirectory(copy_from)
add_subdirectory(copy_to)
add_subdirectory(ddl)
add_subdirectory(hash_join)
add_subdirectory(intersect)
add_subdirectory(order_by)
add_subdirectory(persistent)
add_subdirectory(scan)
add_subdirectory(table_scan)
add_subdirectory(recursive_extend)
add_subdirectory(call)
add_subdirectory(macro)

add_library(kuzu_processor_operator
        OBJECT
        cross_product.cpp
        filter.cpp
        filtering_operator.cpp
        flatten.cpp
        index_scan.cpp
        limit.cpp
        multiplicity_reducer.cpp
        physical_operator.cpp
        projection.cpp
        profile.cpp
        result_collector.cpp
        scan_node_id.cpp
        semi_masker.cpp
        skip.cpp
        unwind.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator>
        PARENT_SCOPE)
