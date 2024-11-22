add_subdirectory(function)
add_subdirectory(catalog)

add_library(fts_extension_main
        OBJECT
        fts_extension.cpp)

set(FTS_OBJECT_FILES
        ${FTS_OBJECT_FILES} $<TARGET_OBJECTS:fts_extension_main>
        PARENT_SCOPE)
