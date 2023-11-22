add_subdirectory(aggregate)
add_subdirectory(arithmetic)
add_subdirectory(cast)
add_subdirectory(table_functions)

add_library(kuzu_function
        OBJECT
        aggregate_function.cpp
        base_lower_upper_operation.cpp
        built_in_functions.cpp
        cast_string_non_nested_functions.cpp
        cast_from_string_functions.cpp
        comparison_functions.cpp
        find_function.cpp
        scalar_macro_function.cpp
        vector_arithmetic_functions.cpp
        vector_boolean_functions.cpp
        vector_cast_functions.cpp
        vector_date_functions.cpp
        vector_hash_functions.cpp
        vector_list_functions.cpp
        vector_null_functions.cpp
        vector_path_functions.cpp
        vector_node_rel_functions.cpp
        vector_string_functions.cpp
        vector_timestamp_functions.cpp
        vector_struct_functions.cpp
        vector_map_functions.cpp
        vector_union_functions.cpp
        vector_blob_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function>
        PARENT_SCOPE)
