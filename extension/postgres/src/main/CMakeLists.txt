add_library(postgres_extension_main
        OBJECT
        postgres_extension.cpp)

set(POSTGRES_EXTENSION_OBJECT_FILES
        ${POSTGRES_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:postgres_extension_main>
        PARENT_SCOPE)
