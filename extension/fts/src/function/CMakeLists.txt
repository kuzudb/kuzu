add_library(kuzu_fts_function
        OBJECT
        stem.cpp
        create_fts_index.cpp
        query_fts_index.cpp
        fts.cpp)

set(FTS_OBJECT_FILES
        ${FTS_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_fts_function>
        PARENT_SCOPE)
