add_library(neo4j_migration_extension_main
        OBJECT
        neo4j_migration_extension.cpp)

set(NEO4J_MIGRATION_EXTENSION_OBJECT_FILES
        ${NEO4J_MIGRATION_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:neo4j_migration_extension_main>
        PARENT_SCOPE)
