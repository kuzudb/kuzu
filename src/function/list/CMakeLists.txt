add_library(kuzu_list_function
        OBJECT
        list_agg_function.cpp
        list_any_value_function.cpp
        list_append_function.cpp
        list_concat_function.cpp
        list_contains_function.cpp
        list_creation.cpp
        list_distinct_function.cpp
        list_extract_function.cpp
        list_range_function.cpp
        list_reverse_function.cpp
        list_slice_function.cpp
        list_sort_function.cpp
        list_to_string_function.cpp
        list_unique_function.cpp
        list_prepend_function.cpp
        list_position_function.cpp
        size_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_list_function>
        PARENT_SCOPE)
