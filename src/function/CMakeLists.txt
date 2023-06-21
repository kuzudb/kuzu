add_subdirectory(comparison)

add_library(kuzu_function
        OBJECT
        aggregate_function.cpp
        base_lower_upper_operation.cpp
        built_in_aggregate_functions.cpp
        built_in_vector_operations.cpp
        find_operation.cpp
        vector_arithmetic_operations.cpp
        vector_boolean_operations.cpp
        vector_cast_operations.cpp
        vector_date_operations.cpp
        vector_hash_operations.cpp
        vector_list_operation.cpp
        vector_null_operations.cpp
        vector_node_rel_operations.cpp
        vector_string_operations.cpp
        vector_timestamp_operations.cpp
        vector_struct_operations.cpp
        vector_map_operation.cpp
        vector_union_operations.cpp
        vector_blob_operations.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function>
        PARENT_SCOPE)
