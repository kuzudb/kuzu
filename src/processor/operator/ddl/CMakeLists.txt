add_library(kuzu_processor_operator_ddl
        OBJECT
        add_node_property.cpp
        add_property.cpp
        add_rel_property.cpp
        create_node_table.cpp
        create_rdf_graph.cpp
        create_rel_table_group.cpp
        create_rel_table.cpp
        ddl.cpp
        drop_property.cpp
        drop_table.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ddl>
        PARENT_SCOPE)
