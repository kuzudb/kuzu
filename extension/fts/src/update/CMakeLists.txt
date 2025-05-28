add_library(kuzu_fts_update
        OBJECT
        update_fts.cpp)

set(FTS_EXTENSION_OBJECT_FILES
        ${FTS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_fts_update>
        PARENT_SCOPE)
