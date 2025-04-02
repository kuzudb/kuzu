add_library(kuzu_neo4j_migration_extension_function
        OBJECT
        neo4j_migrate.cpp)

set(NEO4J_MIGRATION_EXTENSION_OBJECT_FILES
        ${NEO4J_MIGRATION_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_neo4j_migration_extension_function>
        PARENT_SCOPE)
