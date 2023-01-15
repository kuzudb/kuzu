add_library(kuzu_processor_operator_ddl
        OBJECT
        create_node_table.cpp
        create_rel_table.cpp
        ddl.cpp
        drop_table.cpp
        add_property.cpp
        add_node_property.cpp
        add_rel_property.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ddl>
        PARENT_SCOPE)
