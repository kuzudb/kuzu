add_library(kuzu_catalog
        OBJECT
        catalog.cpp
        catalog_content.cpp
        node_table_schema.cpp
        property.cpp
        rdf_graph_schema.cpp
        rel_table_group_schema.cpp
        rel_table_schema.cpp
        table_schema.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_catalog>
        PARENT_SCOPE)
