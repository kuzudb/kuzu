add_library(fts_extension_main
        OBJECT
        fts_extension.cpp)

set(FTS_EXTENSION_OBJECT_FILES
        ${FTS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:fts_extension_main>
        PARENT_SCOPE)
