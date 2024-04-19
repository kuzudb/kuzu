add_library(kuzu_function_union
        OBJECT
        union_extract_function.cpp
        union_tag_function.cpp
        union_value_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_union>
        PARENT_SCOPE)
