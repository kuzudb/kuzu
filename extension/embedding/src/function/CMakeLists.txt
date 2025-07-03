add_library(kuzu_embedding_function
        OBJECT
        create_embedding.cpp)

set(EMBEDDING_EXTENSION_OBJECT_FILES
        ${EMBEDDING_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_embedding_function>
        PARENT_SCOPE)
