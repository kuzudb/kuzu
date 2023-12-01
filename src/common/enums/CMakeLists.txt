add_library(kuzu_common_enums
        OBJECT
        rel_direction.cpp
        table_type.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_enums>
        PARENT_SCOPE)
