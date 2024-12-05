add_library(kuzu_postgres_extension
        OBJECT
        postgres_extension.cpp)

set(POSTGRES_EXTENSION_OBJECT_FILES
        ${POSTGRES_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_postgres_extension>
        PARENT_SCOPE)
