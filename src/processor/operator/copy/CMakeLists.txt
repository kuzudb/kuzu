add_library(kuzu_processor_operator_copy
        OBJECT
        copy.cpp
        copy_node.cpp
        copy_rel.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_copy>
        PARENT_SCOPE)
