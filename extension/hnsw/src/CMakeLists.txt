add_subdirectory(catalog)
add_subdirectory(function)
add_subdirectory(index)

add_library(hnsw_extension_main
        OBJECT
        hnsw_extension.cpp)

set(HNSW_OBJECT_FILES
        ${HNSW_OBJECT_FILES} $<TARGET_OBJECTS:hnsw_extension_main>
        PARENT_SCOPE)
