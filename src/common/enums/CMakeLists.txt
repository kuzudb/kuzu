add_library(kuzu_common_enums
        OBJECT
        path_semantic.cpp
        rel_direction.cpp
        rel_multiplicity.cpp
        table_type.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_enums>
        PARENT_SCOPE)
