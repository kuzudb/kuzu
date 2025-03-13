add_subdirectory(catalog)
add_subdirectory(function)
add_subdirectory(index)

add_library(vector_extension_main
        OBJECT
        vector_extension.cpp)

set(VECTOR_OBJECT_FILES
        ${VECTOR_OBJECT_FILES} $<TARGET_OBJECTS:vector_extension_main>
        PARENT_SCOPE)
