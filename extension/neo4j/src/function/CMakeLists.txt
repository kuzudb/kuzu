add_library(kuzu_neo4j_extension_function
        OBJECT
        neo4j_migrate.cpp)

set(NEO4J_EXTENSION_OBJECT_FILES
        ${NEO4J_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_neo4j_extension_function>
        PARENT_SCOPE)
