add_library(kuzu_catalog
        OBJECT
        catalog.cpp
        catalog_content.cpp
        table_schema.cpp
        property.cpp
        rdf_graph_schema.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_catalog>
        PARENT_SCOPE)
