add_library(vector_extension_main
        OBJECT
        vector_extension.cpp)

set(VECTOR_EXTENSION_OBJECT_FILES
        ${VECTOR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:vector_extension_main>
        PARENT_SCOPE)
