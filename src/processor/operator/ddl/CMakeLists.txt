add_library(kuzu_processor_operator_ddl
        OBJECT
        create_node_table.cpp
        create_rel_table.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ddl>
        PARENT_SCOPE)
