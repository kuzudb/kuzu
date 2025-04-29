add_library(neo4j_extension_main
        OBJECT
        neo4j_extension.cpp)

set(NEO4J_EXTENSION_OBJECT_FILES
        ${NEO4J_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:neo4j_extension_main>
        PARENT_SCOPE)
