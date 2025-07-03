add_library(embedding_extension_main
        OBJECT
        embedding_extension.cpp)

set(EMBEDDING_EXTENSION_OBJECT_FILES
        ${EMBEDDING_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:embedding_extension_main>
        PARENT_SCOPE)
